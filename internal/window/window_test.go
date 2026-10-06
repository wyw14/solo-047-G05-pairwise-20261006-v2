package window

import (
	"errors"
	"strings"
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
func (c *mutableClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

type harness struct {
	clock    *mutableClock
	topology *topology.Registry
	permits  *repair.PermitService
	receipts *landing.ReceiptRegistry
	station  *landing.Station
	service  *WindowService
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	clock := &mutableClock{t: time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register("S4", topology.PathProtection, clock.Now()); err != nil {
		t.Fatal(err)
	}
	receipts := landing.NewReceiptRegistry()
	permits := repair.NewPermitService(feed.NewIsolationService(), receipts, platform.UUIDGenerator{}, clock.Now)
	for _, circuitID := range []string{"A", "B"} {
		circuit, err := feed.NewCircuit(circuitID, "east", clock.Now())
		if err != nil {
			t.Fatal(err)
		}
		permits.RegisterCircuit(circuit)
	}
	station, err := landing.NewStation("east", "East Landing", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	service := NewWindowService(registry, permits, platform.UUIDGenerator{}, clock.Now)
	service.RegisterStation(station)
	return &harness{
		clock: clock, topology: registry, permits: permits, receipts: receipts,
		station: station, service: service,
	}
}

// requestPermit starts the isolation workflow, leaving the circuit isolating.
func (h *harness) requestPermit(t *testing.T, circuitID string) repair.Permit {
	t.Helper()
	permit, err := h.permits.Request("east", circuitID)
	if err != nil {
		t.Fatal(err)
	}
	return permit
}

// ground drives an existing permit through receipt → evaluate → ground.
func (h *harness) ground(t *testing.T, permit repair.Permit) repair.Permit {
	t.Helper()
	h.receipts.Record(landing.IsolationReceipt{
		StationID: permit.StationID, CircuitID: permit.CircuitID, OperationID: permit.OperationID,
		Isolated: true, ReceivedAt: h.clock.Now(),
	})
	permit, err := h.permits.Evaluate(permit.ID)
	if err != nil {
		t.Fatal(err)
	}
	permit, err = h.permits.Ground(permit.ID)
	if err != nil {
		t.Fatal(err)
	}
	return permit
}

// groundedPermit requests a fresh permit and grounds it end to end.
func (h *harness) groundedPermit(t *testing.T, circuitID string) repair.Permit {
	t.Helper()
	return h.ground(t, h.requestPermit(t, circuitID))
}

func (h *harness) planFor(permitID, spanID, circuitID string, start, end time.Time) Plan {
	return Plan{
		SpanID: spanID, StationID: "east", CircuitID: circuitID, PermitID: permitID,
		StartAt: start, EndAt: end, Operator: "rig-7", Notes: "splice repair",
	}
}

func (h *harness) defaultPlan(permitID string) Plan {
	start := h.clock.Now().Add(time.Hour)
	return h.planFor(permitID, "S3", "A", start, start.Add(time.Hour))
}

func asConflict(t *testing.T, err error) *ConflictError {
	t.Helper()
	var conflictErr *ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected *ConflictError, got %v", err)
	}
	return conflictErr
}

func blockerCodes(blockers []Blocker) map[BlockerCode]int {
	counts := make(map[BlockerCode]int)
	for _, blocker := range blockers {
		counts[blocker.Code]++
	}
	return counts
}

func TestSchedulePinsTopologyGeneration(t *testing.T) {
	h := newHarness(t)
	permit := h.requestPermit(t, "A") // still isolating: planning ahead is allowed
	window, err := h.service.Schedule(h.defaultPlan(permit.ID))
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if window.State != StateScheduled {
		t.Fatalf("state=%s", window.State)
	}
	if window.Topology.Generation != 1 || window.Topology.ActivePath != topology.PathPrimary {
		t.Fatalf("pinned topology=%+v", window.Topology)
	}
	if len(h.service.Open()) != 1 {
		t.Fatalf("open windows=%d", len(h.service.Open()))
	}
}

func TestScheduleRejectsMalformedPlan(t *testing.T) {
	h := newHarness(t)
	permit := h.requestPermit(t, "A")
	plan := h.defaultPlan(permit.ID)
	plan.EndAt = plan.StartAt.Add(-time.Minute)
	if blockers := h.service.Preflight(plan); blockers[0].Code != BlockedInvalidWindow {
		t.Fatalf("blockers=%+v", blockers)
	}
	if _, err := h.service.Schedule(plan); err == nil {
		t.Fatal("expected end-before-start error")
	}
	plan = h.defaultPlan(permit.ID)
	plan.PermitID = ""
	if _, err := h.service.Schedule(plan); err == nil {
		t.Fatal("expected missing permit error")
	}
}

func TestScheduleBlockers(t *testing.T) {
	h := newHarness(t)
	_, err := h.service.Schedule(h.defaultPlan("permit-does-not-exist"))
	conflictErr := asConflict(t, err)
	if codes := blockerCodes(conflictErr.Blockers); codes[BlockedPermitNotReady] != 1 {
		t.Fatalf("blockers=%+v", conflictErr.Blockers)
	}

	permit := h.requestPermit(t, "A")
	missingTopology := h.defaultPlan(permit.ID)
	missingTopology.SpanID = "S9"
	conflictErr = asConflict(t, func() error {
		_, err := h.service.Schedule(missingTopology)
		return err
	}())
	if codes := blockerCodes(conflictErr.Blockers); codes[BlockedTopologyMissing] != 1 {
		t.Fatalf("blockers=%+v", conflictErr.Blockers)
	}

	wrongScope := h.planFor(permit.ID, "S4", "B",
		h.clock.Now().Add(2*time.Hour), h.clock.Now().Add(3*time.Hour))
	conflictErr = asConflict(t, func() error {
		_, err := h.service.Schedule(wrongScope)
		return err
	}())
	if codes := blockerCodes(conflictErr.Blockers); codes[BlockedPermitNotReady] != 1 {
		t.Fatalf("scope blockers=%+v", conflictErr.Blockers)
	}
}

func TestOverlapReportsEveryOccupiedLine(t *testing.T) {
	h := newHarness(t)
	permit := h.requestPermit(t, "A")
	first, err := h.service.Schedule(h.defaultPlan(permit.ID))
	if err != nil {
		t.Fatal(err)
	}

	overlapping := h.planFor(permit.ID, "S3", "A",
		first.StartAt.Add(30*time.Minute), first.EndAt.Add(30*time.Minute))
	conflictErr := asConflict(t, func() error {
		_, err := h.service.Schedule(overlapping)
		return err
	}())
	if len(conflictErr.Blockers) != 3 {
		t.Fatalf("expected station+circuit+span blockers, got %+v", conflictErr.Blockers)
	}
	wantLines := map[ResourceKind]string{
		ResourceStation: "east", ResourceCircuit: "east/A", ResourceSpan: "S3",
	}
	for _, blocker := range conflictErr.Blockers {
		if blocker.Code != BlockedWindowOverlap || blocker.WindowID != first.ID {
			t.Fatalf("blocker=%+v", blocker)
		}
		if wantLines[blocker.Kind] != blocker.Line {
			t.Fatalf("line %q for %s", blocker.Line, blocker.Kind)
		}
	}

	// Abutting (not overlapping) intervals are allowed: [1h,2h) then [2h,3h).
	abutting := h.planFor(permit.ID, "S3", "A", first.EndAt, first.EndAt.Add(time.Hour))
	if _, err := h.service.Schedule(abutting); err != nil {
		t.Fatalf("abutting window blocked: %v", err)
	}
}

func TestOverlapIsLineSpecific(t *testing.T) {
	h := newHarness(t)
	permitA := h.requestPermit(t, "A")
	first, err := h.service.Schedule(h.planFor(permitA.ID, "S3", "A",
		h.clock.Now().Add(time.Hour), h.clock.Now().Add(2*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	permitB := h.requestPermit(t, "B")
	// Same station and time, different span and circuit: only the station line clashes.
	other := h.planFor(permitB.ID, "S4", "B", first.StartAt, first.EndAt)
	conflictErr := asConflict(t, func() error {
		_, err := h.service.Schedule(other)
		return err
	}())
	if len(conflictErr.Blockers) != 1 {
		t.Fatalf("expected single station blocker, got %+v", conflictErr.Blockers)
	}
	blocker := conflictErr.Blockers[0]
	if blocker.Kind != ResourceStation || blocker.Line != "east" || blocker.WindowID != first.ID {
		t.Fatalf("blocker=%+v", blocker)
	}
}

func TestPreflightShowsActivationWorkWhileScheduleSucceeds(t *testing.T) {
	h := newHarness(t)
	permit := h.requestPermit(t, "A") // isolating: circuit not yet grounded
	plan := h.defaultPlan(permit.ID)
	blockers := h.service.Preflight(plan)
	codes := blockerCodes(blockers)
	if codes[BlockedCircuitNotGrounded] != 1 || codes[BlockedPermitNotReady] != 1 {
		t.Fatalf("preflight blockers=%+v", blockers)
	}
	if _, err := h.service.Schedule(plan); err != nil {
		t.Fatalf("schedule should succeed despite pending grounding: %v", err)
	}
}

func TestStartGate(t *testing.T) {
	h := newHarness(t)
	grounded := h.groundedPermit(t, "A")
	window, err := h.service.Schedule(h.defaultPlan(grounded.ID))
	if err != nil {
		t.Fatal(err)
	}

	// Too early: timing blocker, but physical readiness (grounded, online) passes.
	conflictErr := asConflict(t, func() error {
		_, err := h.service.Start(window.ID)
		return err
	}())
	codes := blockerCodes(conflictErr.Blockers)
	if codes[BlockedStateConflict] == 0 {
		t.Fatalf("expected timing blocker, got %+v", conflictErr.Blockers)
	}
	if codes[BlockedStationOffline] != 0 || codes[BlockedCircuitNotGrounded] != 0 || codes[BlockedPermitNotReady] != 0 {
		t.Fatalf("physical readiness should already pass: %+v", conflictErr.Blockers)
	}

	// Inside the interval now, but the station went offline.
	h.clock.advance(90 * time.Minute)
	h.station.SetState(landing.StationOffline, h.clock.Now())
	conflictErr = asConflict(t, func() error {
		_, err := h.service.Start(window.ID)
		return err
	}())
	if codes := blockerCodes(conflictErr.Blockers); codes[BlockedStationOffline] != 1 {
		t.Fatalf("offline blockers=%+v", conflictErr.Blockers)
	}

	h.station.SetState(landing.StationOnline, h.clock.Now())
	started, err := h.service.Start(window.ID)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if started.State != StateActive {
		t.Fatalf("state=%s", started.State)
	}
}

func TestStartBlockedByUngroundedIsolation(t *testing.T) {
	h := newHarness(t)
	permit := h.requestPermit(t, "A") // left isolating
	window, err := h.service.Schedule(h.defaultPlan(permit.ID))
	if err != nil {
		t.Fatal(err)
	}
	h.clock.advance(90 * time.Minute)
	conflictErr := asConflict(t, func() error {
		_, err := h.service.Start(window.ID)
		return err
	}())
	codes := blockerCodes(conflictErr.Blockers)
	if codes[BlockedCircuitNotGrounded] != 1 || codes[BlockedPermitNotReady] != 1 {
		t.Fatalf("isolation blockers=%+v", conflictErr.Blockers)
	}
}

func TestStartRejectsElapsedInterval(t *testing.T) {
	h := newHarness(t)
	grounded := h.groundedPermit(t, "A")
	window, err := h.service.Schedule(h.defaultPlan(grounded.ID))
	if err != nil {
		t.Fatal(err)
	}
	h.clock.advance(3 * time.Hour) // past EndAt
	conflictErr := asConflict(t, func() error {
		_, err := h.service.Start(window.ID)
		return err
	}())
	found := false
	for _, blocker := range conflictErr.Blockers {
		if blocker.Code == BlockedStateConflict && strings.Contains(blocker.Reason, "elapsed") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected elapsed-interval blocker, got %+v", conflictErr.Blockers)
	}
}

func TestStartRejectsTopologyGenerationDrift(t *testing.T) {
	h := newHarness(t)
	grounded := h.groundedPermit(t, "A")
	window, err := h.service.Schedule(h.defaultPlan(grounded.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.topology.Switch("S3", topology.PathProtection, h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	h.clock.advance(90 * time.Minute)
	conflictErr := asConflict(t, func() error {
		_, err := h.service.Start(window.ID)
		return err
	}())
	var changed *Blocker
	for i := range conflictErr.Blockers {
		if conflictErr.Blockers[i].Code == BlockedTopologyChanged {
			changed = &conflictErr.Blockers[i]
		}
	}
	if changed == nil || changed.Detail != "pinned=1 live=2" {
		t.Fatalf("drift blockers=%+v", conflictErr.Blockers)
	}
}

func TestCompletionRequiresReceipt(t *testing.T) {
	h := newHarness(t)
	grounded := h.groundedPermit(t, "A")
	window, err := h.service.Schedule(h.defaultPlan(grounded.ID))
	if err != nil {
		t.Fatal(err)
	}

	// A scheduled window cannot complete: it is not active yet.
	if _, err := h.service.Complete(window.ID, CompletionReceipt{ID: "rcpt-1", OTDRTraceID: "scan-9"}); err == nil {
		t.Fatal("expected state-conflict on completing a scheduled window")
	}

	h.clock.advance(90 * time.Minute)
	window, err = h.service.Start(window.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Active window: receipt without an OTDR trace reference is rejected.
	if _, err := h.service.Complete(window.ID, CompletionReceipt{ID: "rcpt-1"}); err == nil {
		t.Fatal("expected missing-OTDR error")
	}

	completed, err := h.service.Complete(window.ID, CompletionReceipt{
		ID: "rcpt-1", OTDRTraceID: "scan-9", Operator: "rig-7", Notes: "bi-directional proof passed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != StateCompleted || completed.Receipt == nil ||
		completed.Receipt.OTDRTraceID != "scan-9" || completed.Receipt.PermitID != grounded.ID {
		t.Fatalf("completed=%+v", completed)
	}
	history := h.service.History()
	if len(history) != 1 || history[0].ID != completed.ID || history[0].Receipt == nil {
		t.Fatalf("history=%+v", history)
	}
	// A completed window cannot be abandoned or completed again.
	if _, err := h.service.Abandon(completed.ID, "too late"); err == nil {
		t.Fatal("expected conflict abandoning a completed window")
	}
}

func TestAbandonRequiresReasonAndFreesLines(t *testing.T) {
	h := newHarness(t)
	permit := h.requestPermit(t, "A")
	window, err := h.service.Schedule(h.defaultPlan(permit.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Abandon(window.ID, "  "); err == nil {
		t.Fatal("expected reason-required error")
	}
	abandoned, err := h.service.Abandon(window.ID, "weather window lost")
	if err != nil {
		t.Fatal(err)
	}
	if abandoned.State != StateAbandoned || abandoned.AbandonReason != "weather window lost" {
		t.Fatalf("abandoned=%+v", abandoned)
	}
	// A terminal window no longer blocks the timeline.
	other, err := h.service.Schedule(h.defaultPlan(permit.ID))
	if err != nil {
		t.Fatalf("re-schedule after abandon: %v", err)
	}
	if open := h.service.Open(); len(open) != 1 || open[0].ID != other.ID {
		t.Fatalf("open=%+v", open)
	}
	if _, err := h.service.Abandon(abandoned.ID, "again"); err == nil {
		t.Fatal("expected conflict on re-abandoning a terminal window")
	}
}

func TestHistoryKeepsPinnedTopologyGeneration(t *testing.T) {
	h := newHarness(t)
	grounded := h.groundedPermit(t, "A")
	window, err := h.service.Schedule(h.defaultPlan(grounded.ID))
	if err != nil {
		t.Fatal(err)
	}
	h.clock.advance(90 * time.Minute)
	if _, err := h.service.Start(window.ID); err != nil {
		t.Fatal(err)
	}
	completed, err := h.service.Complete(window.ID, CompletionReceipt{ID: "rcpt-1", OTDRTraceID: "scan-42"})
	if err != nil {
		t.Fatal(err)
	}

	// The live topology advances after closure; the history record must keep
	// the generation pinned when the window was scheduled.
	if _, err := h.topology.Switch("S3", topology.PathProtection, h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	live, err := h.topology.Snapshot("S3")
	if err != nil {
		t.Fatal(err)
	}
	if live.Generation != 2 {
		t.Fatalf("live generation=%d", live.Generation)
	}
	stored, exists := h.service.Get(completed.ID)
	if !exists {
		t.Fatal("completed window disappeared")
	}
	if stored.Topology.Generation != 1 || stored.Topology.Generation == live.Generation {
		t.Fatalf("history did not preserve pinned generation: %+v live=%d", stored.Topology, live.Generation)
	}
	if stored.Receipt == nil || stored.Receipt.OTDRTraceID != "scan-42" {
		t.Fatalf("history receipt lost: %+v", stored)
	}
}
