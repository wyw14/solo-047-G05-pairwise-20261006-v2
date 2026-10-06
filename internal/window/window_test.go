package window

import (
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/topology"
)

type mutableClock struct{ t time.Time }

func (c *mutableClock) Now() time.Time { return c.t }

type fixture struct {
	svc      *Service
	permits  *repair.PermitService
	receipts *landing.ReceiptRegistry
	topo     *topology.Registry
	station  *landing.Station
	clock    *mutableClock
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clock := &mutableClock{t: time.Date(2026, 10, 6, 8, 0, 0, 0, time.UTC)}
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	receipts := landing.NewReceiptRegistry()
	permits := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now)
	circuit, err := feed.NewCircuit("A", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permits.RegisterCircuit(circuit)
	station, err := landing.NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(registry, permits, receipts, ids, clock.Now)
	svc.RegisterStation(station)
	return &fixture{svc: svc, permits: permits, receipts: receipts, topo: registry, station: station, clock: clock}
}

// groundedPermit drives a permit through isolation, receipt match and grounding.
func groundedPermit(t *testing.T, f *fixture) repair.Permit {
	t.Helper()
	permit, err := f.permits.Request("east", "A")
	if err != nil {
		t.Fatal(err)
	}
	f.receipts.Record(landing.IsolationReceipt{
		StationID: "east", CircuitID: "A", OperationID: permit.OperationID,
		Isolated: true, ReceivedAt: f.clock.Now(),
	})
	if permit, err = f.permits.Evaluate(permit.ID); err != nil {
		t.Fatal(err)
	}
	if permit, err = f.permits.Ground(permit.ID); err != nil {
		t.Fatal(err)
	}
	return permit
}

func baseReq(permitID string) ScheduleRequest {
	return ScheduleRequest{
		SpanID: "S3", StationID: "east", CircuitID: "A", PermitID: permitID,
		StartsAt: time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC),
		EndsAt:   time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC),
		Purpose:  "splice S3",
	}
}

func TestScheduleFreezesTopologyGeneration(t *testing.T) {
	f := newFixture(t)
	permit := groundedPermit(t, f)

	win, err := f.svc.Schedule(baseReq(permit.ID))
	if err != nil {
		t.Fatal(err)
	}
	if win.PlannedGeneration != 1 || win.Topology.Generation != 1 || win.Topology.ActivePath != "primary" {
		t.Fatalf("window did not freeze initial topology: %+v", win.Topology)
	}

	// The live topology advances after the window is scheduled.
	if _, err := f.topo.Open("S3", topology.PathPrimary, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.topo.Connect("S3", topology.PathProtection, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	current, _ := f.topo.Snapshot("S3")
	if current.Generation != 3 {
		t.Fatalf("expected live generation 3, got %d", current.Generation)
	}

	got, exists := f.svc.Get(win.ID)
	if !exists {
		t.Fatal("window missing")
	}
	if got.Topology.Generation != 1 || got.PlannedGeneration != 1 {
		t.Fatalf("historical window lost its topology generation: %+v", got.Topology)
	}
}

func TestScheduleConflictReportsOccupyingLineAndReason(t *testing.T) {
	f := newFixture(t)
	permit := groundedPermit(t, f)

	first, err := f.svc.Schedule(baseReq(permit.ID))
	if err != nil {
		t.Fatal(err)
	}

	// A second window needs its own permit for the same circuit.
	overlap := baseReq(permit.ID)
	overlap.StartsAt = first.StartsAt.Add(30 * time.Minute)
	overlap.EndsAt = first.EndsAt.Add(30 * time.Minute)
	_, err = f.svc.Schedule(overlap)
	var gate *GateError
	if !errors.As(err, &gate) {
		t.Fatalf("expected GateError, got %v", err)
	}
	if len(gate.Blocks) != 1 {
		t.Fatalf("expected one blocking window, got %+v", gate.Blocks)
	}
	block := gate.Blocks[0]
	if block.WindowID != first.ID || block.Reason != ReasonOverlapSameCircuit {
		t.Fatalf("unexpected block: %+v", block)
	}
	if block.Line != "east/A/S3" || block.StationID != "east" || block.CircuitID != "A" {
		t.Fatalf("block must name the occupied line: %+v", block)
	}

	// Touching-but-not-overlapping ranges (half-open) are allowed.
	touching := baseReq(permit.ID)
	touching.StartsAt = first.EndsAt
	touching.EndsAt = first.EndsAt.Add(time.Hour)
	if _, err := f.svc.Schedule(touching); err != nil {
		t.Fatalf("adjacent window should be allowed: %v", err)
	}

	// All open windows close: their lines are released and the earlier
	// overlap is now schedulable.
	for _, open := range f.svc.Open() {
		if _, err := f.svc.Abandon(open.ID, "done"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.svc.Schedule(overlap); err != nil {
		t.Fatalf("closed windows must release the line: %v", err)
	}
}

func TestActivateGates(t *testing.T) {
	f := newFixture(t)

	// Permit not grounded yet -> station passes, permit blocks.
	permit, err := f.permits.Request("east", "A")
	if err != nil {
		t.Fatal(err)
	}
	win, err := f.svc.Schedule(baseReq(permit.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, gate := f.svc.Activate(win.ID); gate == nil || !hasReason(gate.Blocks, ReasonPermitNotGrounded) {
		t.Fatalf("expected permit-not-grounded block, got %+v", gate)
	}
	if stored, _ := f.svc.Get(win.ID); stored.State != StateBlocked {
		t.Fatalf("window should be marked blocked, got %s", stored.State)
	}

	// Ground the same permit, then take the station offline.
	f.receipts.Record(landing.IsolationReceipt{
		StationID: "east", CircuitID: "A", OperationID: permit.OperationID,
		Isolated: true, ReceivedAt: f.clock.Now(),
	})
	if permit, err = f.permits.Evaluate(permit.ID); err != nil {
		t.Fatal(err)
	}
	if permit, err = f.permits.Ground(permit.ID); err != nil {
		t.Fatal(err)
	}
	f.station.SetState(landing.StationOffline, f.clock.Now())
	if _, gate := f.svc.Activate(win.ID); gate == nil || !hasReason(gate.Blocks, ReasonStationOffline) {
		t.Fatalf("expected station-offline block, got %+v", gate)
	}

	// Advance the topology beyond the planned generation.
	f.station.SetState(landing.StationOnline, f.clock.Now())
	if _, err := f.topo.Open("S3", topology.PathPrimary, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, gate := f.svc.Activate(win.ID); gate == nil || !hasReason(gate.Blocks, ReasonTopologyMismatch) {
		t.Fatalf("expected topology-mismatch block, got %+v", gate)
	}

	// Planning against the current generation with all gates green activates.
	current, _ := f.topo.Snapshot("S3")
	req := baseReq(permit.ID)
	req.StartsAt = req.StartsAt.Add(4 * time.Hour)
	req.EndsAt = req.EndsAt.Add(4 * time.Hour)
	fresh, err := f.svc.Schedule(req)
	if err != nil {
		t.Fatal(err)
	}
	active, gate := f.svc.Activate(fresh.ID)
	if gate != nil {
		t.Fatalf("activation should succeed: %+v", gate.Blocks)
	}
	if active.State != StateActive || active.ActivatedGeneration != current.Generation {
		t.Fatalf("unexpected active window: %+v", active)
	}
}

func TestCloseRequiresReceiptOrExplicitAbandon(t *testing.T) {
	f := newFixture(t)
	permit := groundedPermit(t, f)
	win, err := f.svc.Schedule(baseReq(permit.ID))
	if err != nil {
		t.Fatal(err)
	}

	// A window that never ran cannot be completed; it must be abandoned.
	if _, err := f.svc.Complete(win.ID); err == nil {
		t.Fatal("complete of a never-activated window must fail")
	} else {
		var gate *GateError
		if !errors.As(err, &gate) || !hasReason(gate.Blocks, ReasonNotActive) {
			t.Fatalf("expected window-not-active, got %v", err)
		}
	}

	// Activate at the planned start. The grounding receipt predates activation.
	f.clock.t = win.StartsAt
	if _, gate := f.svc.Activate(win.ID); gate != nil {
		t.Fatal(gate)
	}
	if _, err := f.svc.Complete(win.ID); err == nil {
		t.Fatal("active window without post-activation receipt must not close")
	} else {
		var gate *GateError
		if !errors.As(err, &gate) || !hasReason(gate.Blocks, ReasonClosureMissing) {
			t.Fatalf("expected closure-evidence-missing, got %v", err)
		}
	}

	// Hand-back receipt recorded during the window satisfies the closure rule.
	f.clock.t = win.StartsAt.Add(90 * time.Minute)
	f.receipts.Record(landing.IsolationReceipt{
		StationID: "east", CircuitID: "A", OperationID: permit.OperationID,
		Isolated: true, ReceivedAt: f.clock.Now(),
	})
	completed, err := f.svc.Complete(win.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != StateCompleted || completed.Closure == nil || completed.Closure.Abandoned {
		t.Fatalf("window should complete with receipt closure: %+v", completed.Closure)
	}
	if len(f.svc.History()) != 1 || len(f.svc.Open()) != 0 {
		t.Fatal("completed window must move to history")
	}

	// A different window can be explicitly abandoned without a receipt.
	other, err := f.svc.Schedule(baseReq(permit.ID))
	if err != nil {
		t.Fatal(err)
	}
	abandoned, err := f.svc.Abandon(other.ID, "weather window lost")
	if err != nil {
		t.Fatal(err)
	}
	if abandoned.State != StateAbandoned || abandoned.Closure == nil || !abandoned.Closure.Abandoned {
		t.Fatalf("abandoned closure not recorded: %+v", abandoned.Closure)
	}
}

func TestAttachTraceRejectsForeignGeneration(t *testing.T) {
	f := newFixture(t)
	permit := groundedPermit(t, f)
	win, err := f.svc.Schedule(baseReq(permit.ID))
	if err != nil {
		t.Fatal(err)
	}
	active, gate := f.svc.Activate(win.ID)
	if gate != nil {
		t.Fatal(gate)
	}
	if _, err := f.svc.AttachTrace(active.ID, "scan-x", active.PlannedGeneration+1); err == nil {
		t.Fatal("trace from another topology generation must not attach")
	}
	attached, err := f.svc.AttachTrace(active.ID, "scan-x", active.ActivatedGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if attached.OTDRTrace != "scan-x" {
		t.Fatalf("trace not attached: %+v", attached)
	}
}

func TestOverdueWindowsMustClose(t *testing.T) {
	f := newFixture(t)
	permit := groundedPermit(t, f)
	win, err := f.svc.Schedule(baseReq(permit.ID))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.svc.Overdue()) != 0 {
		t.Fatal("window should not be overdue before its end")
	}
	f.clock.t = win.EndsAt.Add(time.Minute)
	overdue := f.svc.Overdue()
	if len(overdue) != 1 || overdue[0].ID != win.ID {
		t.Fatalf("expected the unclosed window to be overdue: %+v", overdue)
	}
	if _, err := f.svc.Abandon(win.ID, "no work performed"); err != nil {
		t.Fatal(err)
	}
	if len(f.svc.Overdue()) != 0 {
		t.Fatal("closed window must leave the overdue list")
	}
}

func hasReason(blocks []Block, reason Reason) bool {
	for _, block := range blocks {
		if block.Reason == reason {
			return true
		}
	}
	return false
}
