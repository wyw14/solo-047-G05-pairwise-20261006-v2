package window

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/topology"
)

// ConflictError is returned when a window cannot be scheduled or started. The
// blockers list every occupied line and every unmet prerequisite.
type ConflictError struct {
	Blockers []Blocker `json:"blockers"`
}

func (e *ConflictError) Error() string {
	parts := make([]string, 0, len(e.Blockers))
	for _, blocker := range e.Blockers {
		if blocker.Line != "" {
			parts = append(parts, fmt.Sprintf("%s on %s %s: %s", blocker.Code, blocker.Kind, blocker.Line, blocker.Reason))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s: %s", blocker.Code, blocker.Reason))
	}
	return "maintenance window blocked: " + strings.Join(parts, "; ")
}

// WindowService combines station online state, feed isolation, OTDR topology
// generations and repair permits into schedulable maintenance windows.
//
// The four inputs are enforced at two gates:
//
//   - Schedule gate (plannable): the permit exists and is scoped to the
//     window's station/circuit, the span has a topology whose generation is
//     pinned onto the window, and no open window occupies the same lines over
//     the requested interval. Physical isolation has not happened yet because
//     windows are normally planned for a future interval.
//   - Start gate (work begins): the station reports online, the feed circuit
//     is isolated and grounded, the permit is grounded, and the live topology
//     generation still matches the generation pinned at schedule time.
type WindowService struct {
	mu       sync.RWMutex
	windows  map[string]Window
	stations map[string]*landing.Station
	topology *topology.Registry
	permits  *repair.PermitService
	ids      platform.IDGenerator
	now      func() time.Time
}

func NewWindowService(topologies *topology.Registry, permits *repair.PermitService, ids platform.IDGenerator, now func() time.Time) *WindowService {
	return &WindowService{
		windows:  make(map[string]Window),
		stations: make(map[string]*landing.Station),
		topology: topologies,
		permits:  permits,
		ids:      ids,
		now:      now,
	}
}

// RegisterStation makes a landing station's online state available for windows.
func (s *WindowService) RegisterStation(station *landing.Station) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stations[station.Snapshot().ID] = station
}

// Schedule validates the plan and the schedule gate (permit scope, topology
// availability, timeline conflicts), then pins the current topology
// generation onto the new window.
func (s *WindowService) Schedule(plan Plan) (Window, error) {
	at := s.now().UTC()
	if err := plan.validate(at); err != nil {
		return Window{}, err
	}
	plan.StartAt = plan.StartAt.UTC()
	plan.EndAt = plan.EndAt.UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	if blockers := s.scheduleBlockersLocked(plan); len(blockers) > 0 {
		return Window{}, &ConflictError{Blockers: blockers}
	}
	topo, err := s.topology.Snapshot(plan.SpanID)
	if err != nil {
		return Window{}, err
	}
	window := Window{
		ID:        s.ids.New("window"),
		SpanID:    plan.SpanID,
		StationID: plan.StationID,
		CircuitID: plan.CircuitID,
		PermitID:  plan.PermitID,
		Operator:  plan.Operator,
		Notes:     plan.Notes,
		State:     StateScheduled,
		StartAt:   plan.StartAt,
		EndAt:     plan.EndAt,
		Topology: TopologyGeneration{
			SpanID:      topo.SpanID,
			Generation:  topo.Generation,
			ActivePath:  topo.ActivePath,
			PrimaryOpen: topo.PrimaryOpen,
			ProtectOpen: topo.ProtectOpen,
			CapturedAt:  at,
		},
		CreatedAt: at,
		UpdatedAt: at,
	}
	s.windows[window.ID] = window
	return window, nil
}

// Preflight reports every blocker a plan would hit: the schedule gate plus the
// activation gate evaluated against the current physical state. It creates no
// window, so callers can see future ground-work (grounding, station comms)
// that still has to be completed before the window opens.
func (s *WindowService) Preflight(plan Plan) []Blocker {
	if err := plan.validate(s.now().UTC()); err != nil {
		return []Blocker{{Code: BlockedInvalidWindow, Reason: err.Error()}}
	}
	plan.StartAt = plan.StartAt.UTC()
	plan.EndAt = plan.EndAt.UTC()
	s.mu.RLock()
	defer s.mu.RUnlock()
	blockers := s.scheduleBlockersLocked(plan)
	blockers = append(blockers, s.activationReadinessLocked(plan.StationID, plan.CircuitID, plan.PermitID)...)
	return blockers
}

// Start moves a scheduled window into its active run interval after enforcing
// the start gate: online station, grounded circuit and permit, unchanged
// topology generation, and a current interval.
func (s *WindowService) Start(windowID string) (Window, error) {
	at := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[windowID]
	if !exists {
		return Window{}, fmt.Errorf("window %s not found", windowID)
	}
	if window.State != StateScheduled {
		return Window{}, &ConflictError{Blockers: []Blocker{{
			Code: BlockedStateConflict, Kind: ResourceSpan, Line: spanLine(window.SpanID),
			Reason: fmt.Sprintf("window is %s, only scheduled windows can start", window.State),
		}}}
	}
	var blockers []Blocker
	if at.Before(window.StartAt) {
		blockers = append(blockers, Blocker{
			Code: BlockedStateConflict, Kind: ResourceSpan, Line: spanLine(window.SpanID),
			Reason: fmt.Sprintf("window opens at %s", window.StartAt.Format(time.RFC3339)),
		})
	}
	if !at.Before(window.EndAt) {
		blockers = append(blockers, Blocker{
			Code: BlockedStateConflict, Kind: ResourceSpan, Line: spanLine(window.SpanID),
			Reason: fmt.Sprintf("scheduled interval elapsed at %s; window must be abandoned", window.EndAt.Format(time.RFC3339)),
		})
	}
	blockers = append(blockers, s.activationReadinessLocked(window.StationID, window.CircuitID, window.PermitID)...)
	blockers = append(blockers, s.generationLocked(window)...)
	if len(blockers) > 0 {
		return Window{}, &ConflictError{Blockers: blockers}
	}
	window.State = StateActive
	window.StartedAt = at
	window.UpdatedAt = at
	s.windows[windowID] = window
	return window, nil
}

// Complete closes an active window. A completion receipt referencing the OTDR
// test and permit is mandatory; without one the window must be Abandoned.
func (s *WindowService) Complete(windowID string, receipt CompletionReceipt) (Window, error) {
	if receipt.ID == "" {
		return Window{}, fmt.Errorf("completion receipt id is required")
	}
	if receipt.OTDRTraceID == "" {
		return Window{}, fmt.Errorf("completion receipt must reference an OTDR test trace")
	}
	at := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[windowID]
	if !exists {
		return Window{}, fmt.Errorf("window %s not found", windowID)
	}
	if window.State != StateActive {
		return Window{}, &ConflictError{Blockers: []Blocker{{
			Code: BlockedStateConflict, Kind: ResourceSpan, Line: spanLine(window.SpanID),
			Reason: fmt.Sprintf("window is %s, only active windows can complete", window.State),
		}}}
	}
	receipt.StationID = window.StationID
	receipt.PermitID = window.PermitID
	if receipt.RecordedAt.IsZero() {
		receipt.RecordedAt = at
	} else {
		receipt.RecordedAt = receipt.RecordedAt.UTC()
	}
	stored := receipt
	window.State = StateCompleted
	window.Receipt = &stored
	window.ClosedAt = at
	window.UpdatedAt = at
	s.windows[windowID] = window
	return window, nil
}

// Abandon explicitly gives a window up. A reason is mandatory so windows are
// never closed without either a receipt or a recorded waiver.
func (s *WindowService) Abandon(windowID, reason string) (Window, error) {
	if strings.TrimSpace(reason) == "" {
		return Window{}, fmt.Errorf("abandon reason is required")
	}
	at := s.now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	window, exists := s.windows[windowID]
	if !exists {
		return Window{}, fmt.Errorf("window %s not found", windowID)
	}
	if !window.open() {
		return Window{}, &ConflictError{Blockers: []Blocker{{
			Code: BlockedStateConflict, Kind: ResourceSpan, Line: spanLine(window.SpanID),
			Reason: fmt.Sprintf("window is already %s", window.State),
		}}}
	}
	window.State = StateAbandoned
	window.AbandonReason = reason
	window.ClosedAt = at
	window.UpdatedAt = at
	s.windows[windowID] = window
	return window, nil
}

func (s *WindowService) Get(windowID string) (Window, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	window, exists := s.windows[windowID]
	return window, exists
}

// List returns every window in start-time order, including closed history.
func (s *WindowService) List() []Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	windows := make([]Window, 0, len(s.windows))
	for _, window := range s.windows {
		windows = append(windows, window)
	}
	sort.Slice(windows, func(i, j int) bool {
		if !windows[i].StartAt.Equal(windows[j].StartAt) {
			return windows[i].StartAt.Before(windows[j].StartAt)
		}
		return windows[i].ID < windows[j].ID
	})
	return windows
}

// Open returns windows still occupying their lines (scheduled or active).
func (s *WindowService) Open() []Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var windows []Window
	for _, window := range s.windows {
		if window.open() {
			windows = append(windows, window)
		}
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].StartAt.Before(windows[j].StartAt) })
	return windows
}

// History returns terminal windows, most recently closed first. Their pinned
// topology generation is kept on the record even after the live topology has
// advanced.
func (s *WindowService) History() []Window {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var windows []Window
	for _, window := range s.windows {
		if window.terminal() {
			windows = append(windows, window)
		}
	}
	sort.Slice(windows, func(i, j int) bool {
		if !windows[i].ClosedAt.Equal(windows[j].ClosedAt) {
			return windows[i].ClosedAt.After(windows[j].ClosedAt)
		}
		return windows[i].ID < windows[j].ID
	})
	return windows
}

// scheduleBlockersLocked enforces the planning-time gate: permit existence and
// scope, a registered topology to pin, and a conflict-free timeline.
// Callers must hold s.mu (at least read-locked).
func (s *WindowService) scheduleBlockersLocked(plan Plan) []Blocker {
	var blockers []Blocker
	if _, err := s.topology.Snapshot(plan.SpanID); err != nil {
		blockers = append(blockers, Blocker{
			Code: BlockedTopologyMissing, Kind: ResourceSpan, Line: spanLine(plan.SpanID),
			Reason: err.Error(),
		})
	}
	blockers = append(blockers, s.permitScopeLocked(plan.StationID, plan.CircuitID, plan.PermitID)...)
	blockers = append(blockers, s.overlapsLocked(plan)...)
	return blockers
}

// permitScopeLocked checks that the permit exists and belongs to this window's
// station/circuit. Its grounding state is only enforced at the start gate.
func (s *WindowService) permitScopeLocked(stationID, circuitID, permitID string) []Blocker {
	permit, exists := s.permits.Get(permitID)
	if !exists {
		return []Blocker{{
			Code:   BlockedPermitNotReady,
			Reason: fmt.Sprintf("repair permit %s is not registered", permitID),
		}}
	}
	if permit.StationID == stationID && permit.CircuitID == circuitID {
		return nil
	}
	return []Blocker{{
		Code: BlockedPermitNotReady, Kind: ResourceCircuit, Line: circuitLine(stationID, circuitID),
		WindowID: permitID,
		Reason:   fmt.Sprintf("permit is scoped to %s/%s, not this window's circuit", permit.StationID, permit.CircuitID),
	}}
}

// activationReadinessLocked enforces the physical start gate: online station,
// grounded feed circuit, grounded and correctly scoped permit. Callers must
// hold s.mu (at least read-locked).
func (s *WindowService) activationReadinessLocked(stationID, circuitID, permitID string) []Blocker {
	var blockers []Blocker
	station := s.stations[stationID]
	if station == nil {
		blockers = append(blockers, Blocker{
			Code: BlockedStationOffline, Kind: ResourceStation, Line: stationLine(stationID),
			Reason: "landing station is not registered for windows",
		})
	} else if snap := station.Snapshot(); snap.State != landing.StationOnline {
		blockers = append(blockers, Blocker{
			Code: BlockedStationOffline, Kind: ResourceStation, Line: stationLine(stationID),
			Reason: fmt.Sprintf("station reports %s (last seen %s); online state is required", snap.State, snap.LastSeenAt.Format(time.RFC3339)),
			Detail: string(snap.State),
		})
	}
	circuit, exists := s.permits.Circuit(stationID, circuitID)
	if !exists {
		blockers = append(blockers, Blocker{
			Code: BlockedCircuitNotGrounded, Kind: ResourceCircuit, Line: circuitLine(stationID, circuitID),
			Reason: "feed circuit is not registered",
		})
	} else if snap := circuit.Snapshot(); snap.State != feed.StateGrounded {
		blockers = append(blockers, Blocker{
			Code: BlockedCircuitNotGrounded, Kind: ResourceCircuit, Line: circuitLine(stationID, circuitID),
			Reason: fmt.Sprintf("feed circuit is %s; it must be isolated and grounded", snap.State),
			Detail: string(snap.State),
		})
	}
	if permit, exists := s.permits.Get(permitID); exists {
		if permit.StationID == stationID && permit.CircuitID == circuitID && permit.State != repair.PermitGrounded {
			blockers = append(blockers, Blocker{
				Code: BlockedPermitNotReady, Kind: ResourceCircuit, Line: circuitLine(stationID, circuitID),
				WindowID: permitID, Detail: string(permit.State),
				Reason: fmt.Sprintf("repair permit is %s; a grounded permit is required", permit.State),
			})
		}
	}
	return blockers
}

// generationLocked fails start if the live topology moved since scheduling.
// Callers must hold s.mu (at least read-locked).
func (s *WindowService) generationLocked(window Window) []Blocker {
	topo, err := s.topology.Snapshot(window.SpanID)
	if err != nil {
		return []Blocker{{
			Code: BlockedTopologyMissing, Kind: ResourceSpan, Line: spanLine(window.SpanID),
			Reason: err.Error(),
		}}
	}
	if topo.Generation != window.Topology.Generation {
		return []Blocker{{
			Code: BlockedTopologyChanged, Kind: ResourceSpan, Line: spanLine(window.SpanID),
			WindowID: window.ID,
			Reason: fmt.Sprintf("topology generation advanced from %d to %d after the window was scheduled; abandon and re-schedule",
				window.Topology.Generation, topo.Generation),
			Detail: fmt.Sprintf("pinned=%d live=%d", window.Topology.Generation, topo.Generation),
		}}
	}
	return nil
}

// overlapsLocked scans open windows for time-overlapping occupation of any of
// the plan's lines. Callers must hold s.mu (at least read-locked).
func (s *WindowService) overlapsLocked(plan Plan) []Blocker {
	wanted := Window{StationID: plan.StationID, CircuitID: plan.CircuitID, SpanID: plan.SpanID}
	lines := wanted.occupiedLines()
	var blockers []Blocker
	for _, other := range s.windows {
		if !other.open() || !other.overlaps(plan.StartAt, plan.EndAt) {
			continue
		}
		for key, kind := range other.occupiedLines() {
			if _, taken := lines[key]; !taken {
				continue
			}
			line := key[strings.IndexByte(key, '\x00')+1:]
			blockers = append(blockers, Blocker{
				Code: BlockedWindowOverlap, Kind: kind, Line: line, WindowID: other.ID,
				Reason: fmt.Sprintf("occupied by %s window %s from %s to %s",
					other.State, other.ID, other.StartAt.Format(time.RFC3339), other.EndAt.Format(time.RFC3339)),
			})
		}
	}
	sortBlockers(blockers)
	return blockers
}

func sortBlockers(blockers []Blocker) {
	sort.Slice(blockers, func(i, j int) bool {
		if blockers[i].Kind != blockers[j].Kind {
			return blockers[i].Kind < blockers[j].Kind
		}
		if blockers[i].Line != blockers[j].Line {
			return blockers[i].Line < blockers[j].Line
		}
		return blockers[i].WindowID < blockers[j].WindowID
	})
}
