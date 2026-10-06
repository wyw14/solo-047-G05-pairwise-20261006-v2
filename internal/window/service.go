package window

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/topology"
)

// GateError reports the gate conditions that prevented activation.
type GateError struct {
	Blocks []Block
}

func (e *GateError) Error() string {
	return fmt.Sprintf("window activation blocked by %d condition(s)", len(e.Blocks))
}

// Service composes station online state, feed isolation permits, OTDR tests and
// topology generations into schedulable maintenance windows.
type Service struct {
	mu       sync.RWMutex
	windows  map[string]Window
	stations map[string]*landing.Station

	topology *topology.Registry
	permits  *repair.PermitService
	receipts *landing.ReceiptRegistry
	ids      platform.IDGenerator
	now      func() time.Time
}

func NewService(reg *topology.Registry, permits *repair.PermitService, receipts *landing.ReceiptRegistry,
	ids platform.IDGenerator, now func() time.Time) *Service {
	return &Service{
		windows:  make(map[string]Window),
		stations: make(map[string]*landing.Station),
		topology: reg, permits: permits, receipts: receipts, ids: ids, now: now,
	}
}

// RegisterStation makes a landing station available for online-state gates.
func (s *Service) RegisterStation(station *landing.Station) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stations[station.Snapshot().ID] = station
}

// ScheduleRequest carries the plan for a new window.
type ScheduleRequest struct {
	SpanID    string
	StationID string
	CircuitID string
	PermitID  string
	StartsAt  time.Time
	EndsAt    time.Time
	Purpose   string
}

// Schedule reserves a window. The topology generation is frozen at scheduling
// time so historical windows keep the generation they were planned against.
func (s *Service) Schedule(req ScheduleRequest) (Window, error) {
	if err := validateSchedule(req.SpanID, req.StationID, req.CircuitID, req.StartsAt, req.EndsAt); err != nil {
		return Window{}, err
	}
	permit, exists := s.permits.Get(req.PermitID)
	if !exists {
		return Window{}, fmt.Errorf("permit %s not found", req.PermitID)
	}
	if permit.StationID != req.StationID || permit.CircuitID != req.CircuitID {
		return Window{}, fmt.Errorf("permit %s does not cover circuit %s/%s", req.PermitID, req.StationID, req.CircuitID)
	}
	topo, err := s.topology.Snapshot(req.SpanID)
	if err != nil {
		return Window{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if blocks := s.conflictsLocked(req.SpanID, req.StationID, req.CircuitID, req.StartsAt, req.EndsAt, ""); len(blocks) > 0 {
		return Window{}, &GateError{Blocks: blocks}
	}
	now := s.now().UTC()
	window := Window{
		ID: s.ids.New("window"), SpanID: req.SpanID, StationID: req.StationID, CircuitID: req.CircuitID,
		PermitID: req.PermitID, StartsAt: req.StartsAt.UTC(), EndsAt: req.EndsAt.UTC(), Purpose: req.Purpose,
		State: StateScheduled, PlannedGeneration: topo.Generation, Topology: freezeTopology(topo),
		CreatedAt: now, UpdatedAt: now,
	}
	s.windows[window.ID] = window
	return window, nil
}

// conflictsLocked inspects every non-terminal window for a time-overlapping
// resource occupation and reports the occupying line plus a block reason.
func (s *Service) conflictsLocked(spanID, stationID, circuitID string, start, end time.Time, ignore string) []Block {
	seen := make(map[string]bool)
	var blocks []Block
	for _, existing := range s.windows {
		if existing.ID == ignore || existing.State.terminal() {
			continue
		}
		if !overlaps(start, end, existing.StartsAt, existing.EndsAt) {
			continue
		}
		reason, shared := overlapReason(existing, spanID, stationID, circuitID)
		if !shared || seen[existing.ID] {
			continue
		}
		seen[existing.ID] = true
		blocks = append(blocks, Block{
			Reason: reason, Detail: "window " + existing.ID + " occupies the line " + timeRange(existing),
			WindowID: existing.ID, StationID: existing.StationID, CircuitID: existing.CircuitID,
			SpanID: existing.SpanID, Line: existing.Line(), TopologyGen: existing.ActivatedOrPlannedGeneration(),
		})
	}
	sort.SliceStable(blocks, func(i, j int) bool { return blocks[i].WindowID < blocks[j].WindowID })
	return blocks
}

func overlapReason(existing Window, spanID, stationID, circuitID string) (Reason, bool) {
	switch {
	case existing.StationID == stationID && existing.CircuitID == circuitID:
		return ReasonOverlapSameCircuit, true
	case existing.SpanID == spanID:
		return ReasonOverlapSameSpan, true
	case existing.StationID == stationID:
		return ReasonOverlapSameStation, true
	}
	return "", false
}

func timeRange(w Window) string {
	return w.StartsAt.Format(time.RFC3339) + ".." + w.EndsAt.Format(time.RFC3339)
}

// ActivatedOrPlannedGeneration returns the generation a window is effectively pinned to.
func (w Window) ActivatedOrPlannedGeneration() uint64 {
	if w.ActivatedGeneration != 0 {
		return w.ActivatedGeneration
	}
	return w.PlannedGeneration
}

// Activate moves a scheduled window into active work once every gate passes.
// On failure the window is marked blocked and the GateError explains why.
func (s *Service) Activate(id string) (Window, *GateError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[id]
	if !exists {
		return Window{}, &GateError{Blocks: []Block{{Reason: ReasonNotFound, Detail: id}}}
	}
	if window.State.terminal() {
		return window, &GateError{Blocks: []Block{{Reason: ReasonAlreadyTerminal, WindowID: id, Detail: "window is already closed"}}}
	}

	var blocks []Block
	if station := s.stations[window.StationID]; station == nil {
		blocks = append(blocks, Block{Reason: ReasonStationOffline, StationID: window.StationID, Detail: "station is not registered"})
	} else {
		switch snap := station.Snapshot(); snap.State {
		case landing.StationOnline:
		case landing.StationDegraded:
			blocks = append(blocks, Block{Reason: ReasonStationDegraded, StationID: window.StationID, Detail: "station state=" + string(snap.State)})
		default:
			blocks = append(blocks, Block{Reason: ReasonStationOffline, StationID: window.StationID, Detail: "station state=" + string(snap.State)})
		}
	}

	permit, ok := s.permits.Get(window.PermitID)
	if !ok {
		blocks = append(blocks, Block{Reason: ReasonPermitNotGrounded, WindowID: id, Detail: "permit missing"})
	} else if permit.State != repair.PermitGrounded {
		blocks = append(blocks, Block{Reason: ReasonPermitNotGrounded, WindowID: id, Detail: "permit state=" + string(permit.State)})
	}

	topo, err := s.topology.Snapshot(window.SpanID)
	if err != nil {
		blocks = append(blocks, Block{Reason: ReasonTopologyMismatch, SpanID: window.SpanID, Detail: err.Error()})
	} else if topo.Generation != window.PlannedGeneration {
		blocks = append(blocks, Block{
			Reason: ReasonTopologyMismatch, SpanID: window.SpanID, TopologyGen: topo.Generation,
			Detail: fmt.Sprintf("topology advanced to generation %d, window planned on %d", topo.Generation, window.PlannedGeneration),
		})
	}

	if blocks = append(blocks, s.conflictsLocked(window.SpanID, window.StationID, window.CircuitID, window.StartsAt, window.EndsAt, window.ID)...); len(blocks) > 0 {
		window.State = StateBlocked
		window.Blocks = blocks
		window.UpdatedAt = s.now().UTC()
		s.windows[id] = window
		return window, &GateError{Blocks: blocks}
	}

	activated := s.now().UTC()
	window.State = StateActive
	window.Blocks = nil
	window.ActivatedGeneration = topo.Generation
	window.ActivatedAt = &activated
	window.UpdatedAt = activated
	s.windows[id] = window
	return window, nil
}

// AttachTrace binds an OTDR trace to an active window. The trace must have been
// collected on the same topology generation the window activated on.
func (s *Service) AttachTrace(id, traceID string, generation uint64) (Window, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[id]
	if !exists {
		return Window{}, fmt.Errorf("window %s not found", id)
	}
	if window.State.terminal() {
		return Window{}, fmt.Errorf("window %s is already closed", id)
	}
	if window.State != StateActive {
		return Window{}, &GateError{Blocks: []Block{{Reason: ReasonNotActive, WindowID: id, Detail: "state=" + string(window.State)}}}
	}
	if generation != window.ActivatedGeneration {
		return Window{}, &GateError{Blocks: []Block{{
			Reason: ReasonTopologyMismatch, WindowID: id, TopologyGen: generation,
			Detail: fmt.Sprintf("trace generation %d does not match active generation %d", generation, window.ActivatedGeneration),
		}}}
	}
	window.OTDRTrace = traceID
	window.UpdatedAt = s.now().UTC()
	s.windows[id] = window
	return window, nil
}

// Complete closes an active window against an isolation receipt that was
// recorded after work began. A window that never ran must be Abandoned instead.
// Without the receipt the active window cannot close.
func (s *Service) Complete(id string) (Window, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[id]
	if !exists {
		return Window{}, fmt.Errorf("window %s not found", id)
	}
	if window.State.terminal() {
		return Window{}, fmt.Errorf("window %s is already closed", id)
	}
	if window.State != StateActive {
		return Window{}, &GateError{Blocks: []Block{{
			Reason: ReasonNotActive, WindowID: id, Detail: "window was never activated; record an explicit abandonment instead",
		}}}
	}
	permit, ok := s.permits.Get(window.PermitID)
	if !ok {
		return Window{}, fmt.Errorf("permit %s not found", window.PermitID)
	}
	receipt, matched := s.receipts.Match(window.StationID, window.CircuitID, permit.OperationID)
	// The pre-work isolation receipt proves grounding; closure needs a receipt
	// confirming isolation at/after activation (the post-work hand-back).
	if !matched || window.ActivatedAt == nil || !receipt.ReceivedAt.After(*window.ActivatedAt) {
		return Window{}, &GateError{Blocks: []Block{{
			Reason: ReasonClosureMissing, WindowID: id, CircuitID: window.CircuitID, StationID: window.StationID,
			Detail: "no post-activation isolated receipt for operation " + permit.OperationID + "; record a receipt or explicitly abandon the window",
		}}}
	}
	closed := s.now().UTC()
	window.State = StateCompleted
	window.Blocks = nil
	window.Closure = &Closure{
		ReceiptID: permit.OperationID, StationID: receipt.StationID, CircuitID: receipt.CircuitID,
		Operation: permit.OperationID, RecordedAt: receipt.ReceivedAt.UTC(),
	}
	window.ClosedAt = &closed
	window.UpdatedAt = closed
	s.windows[id] = window
	return window, nil
}

// Abandon is the explicit waiver: the window closes without a receipt.
func (s *Service) Abandon(id, reason string) (Window, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[id]
	if !exists {
		return Window{}, fmt.Errorf("window %s not found", id)
	}
	if window.State.terminal() {
		return Window{}, fmt.Errorf("window %s is already closed", id)
	}
	closed := s.now().UTC()
	window.State = StateAbandoned
	window.Blocks = nil
	window.Closure = &Closure{Abandoned: true, Reason: reason, RecordedAt: closed}
	window.ClosedAt = &closed
	window.UpdatedAt = closed
	s.windows[id] = window
	return window, nil
}

// Reschedule moves a non-terminal window to a new time range, re-checking conflicts.
func (s *Service) Reschedule(id string, start, end time.Time) (Window, error) {
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return Window{}, fmt.Errorf("window end must be after its start")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[id]
	if !exists {
		return Window{}, fmt.Errorf("window %s not found", id)
	}
	if window.State.terminal() {
		return Window{}, fmt.Errorf("window %s is already closed", id)
	}
	if blocks := s.conflictsLocked(window.SpanID, window.StationID, window.CircuitID, start, end, id); len(blocks) > 0 {
		return Window{}, &GateError{Blocks: blocks}
	}
	window.StartsAt = start.UTC()
	window.EndsAt = end.UTC()
	if window.State == StateBlocked {
		window.State = StateScheduled
		window.Blocks = nil
	}
	window.UpdatedAt = s.now().UTC()
	s.windows[id] = window
	return window, nil
}

func (s *Service) Get(id string) (Window, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	window, exists := s.windows[id]
	return window, exists
}

func (s *Service) List() []Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sortedLocked(func(w Window) bool { return true })
}

// Open returns windows that have not yet closed (scheduled/blocked/active).
func (s *Service) Open() []Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sortedLocked(func(w Window) bool { return !w.State.terminal() })
}

// History returns closed windows. Each carries the topology generation frozen
// at scheduling time, independent of later topology changes.
func (s *Service) History() []Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sortedLocked(func(w Window) bool { return w.State.terminal() })
}

// Overdue returns open windows whose planned end has passed. Such a window is
// in breach of the closure rule until a receipt (Complete) or a waiver
// (Abandon) is recorded.
func (s *Service) Overdue() []Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	return s.sortedLocked(func(w Window) bool { return !w.State.terminal() && !w.EndsAt.After(now) })
}

func (s *Service) sortedLocked(include func(Window) bool) []Window {
	result := make([]Window, 0, len(s.windows))
	for _, window := range s.windows {
		if include(window) {
			result = append(result, window)
		}
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].StartsAt.Before(result[j].StartsAt) })
	return result
}

func freezeTopology(topo topology.Snapshot) TopologyView {
	return TopologyView{
		SpanID: topo.SpanID, Generation: topo.Generation, ActivePath: string(topo.ActivePath),
		PrimaryOpen: topo.PrimaryOpen, ProtectOpen: topo.ProtectOpen,
	}
}
