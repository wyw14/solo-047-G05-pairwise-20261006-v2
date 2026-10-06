package window

import (
	"errors"
	"strings"
	"time"
)

// State is the lifecycle state of a maintenance window.
type State string

const (
	// StateScheduled: planned, not yet active.
	StateScheduled State = "scheduled"
	// StateBlocked: activation attempted but gate conditions were not satisfied.
	StateBlocked State = "blocked"
	// StateActive: crew is permitted to work; the OTDR test may run.
	StateActive State = "active"
	// StateCompleted: window closed against a matching isolation receipt.
	StateCompleted State = "completed"
	// StateAbandoned: window explicitly waived, no receipt required.
	StateAbandoned State = "abandoned"
)

func (s State) terminal() bool {
	return s == StateCompleted || s == StateAbandoned
}

// Reason explains why a window is blocked or conflicts with another one.
type Reason string

const (
	ReasonStationOffline     Reason = "station-offline"
	ReasonStationDegraded    Reason = "station-degraded"
	ReasonPermitNotGrounded  Reason = "permit-not-grounded"
	ReasonTopologyMismatch   Reason = "topology-generation-mismatch"
	ReasonOverlapSameCircuit Reason = "overlap-circuit-busy"
	ReasonOverlapSameSpan    Reason = "overlap-span-busy"
	ReasonOverlapSameStation Reason = "overlap-station-busy"
	ReasonNotFound           Reason = "window-not-found"
	ReasonNotActive          Reason = "window-not-active"
	ReasonAlreadyTerminal    Reason = "window-already-closed"
	ReasonClosureMissing     Reason = "closure-evidence-missing"
)

// Block describes a single failed gate or an occupying window during scheduling.
type Block struct {
	Reason      Reason `json:"reason"`
	Detail      string `json:"detail,omitempty"`
	WindowID    string `json:"window_id,omitempty"`
	StationID   string `json:"station_id,omitempty"`
	CircuitID   string `json:"circuit_id,omitempty"`
	SpanID      string `json:"span_id,omitempty"`
	Line        string `json:"line,omitempty"`
	TopologyGen uint64 `json:"topology_generation,omitempty"`
}

// Closure is the evidence or waiver attached when a window closes.
type Closure struct {
	ReceiptID  string    `json:"receipt_id,omitempty"`
	StationID  string    `json:"station_id,omitempty"`
	CircuitID  string    `json:"circuit_id,omitempty"`
	Operation  string    `json:"operation,omitempty"`
	Abandoned  bool      `json:"abandoned"`
	Reason     string    `json:"reason,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

// Window is a schedulable work window combining station online state, feed
// isolation, OTDR testing and the repair permit.
type Window struct {
	ID        string `json:"id"`
	SpanID    string `json:"span_id"`
	StationID string `json:"station_id"`
	CircuitID string `json:"circuit_id"`
	PermitID  string `json:"permit_id"`

	StartsAt time.Time `json:"starts_at"`
	EndsAt   time.Time `json:"ends_at"`
	Purpose  string    `json:"purpose"`

	State     State    `json:"state"`
	Blocks    []Block  `json:"blocks,omitempty"`
	OTDRTrace string   `json:"otdr_trace_id,omitempty"`
	Closure   *Closure `json:"closure,omitempty"`

	// PlannedGeneration is the topology generation the plan was authored against.
	PlannedGeneration uint64 `json:"planned_generation"`
	// ActivatedGeneration is the generation observed when work actually started.
	ActivatedGeneration uint64 `json:"activated_generation,omitempty"`
	// Topology freezes the topology as it was at scheduling time. Historical
	// windows keep that generation even after the live topology moves on.
	Topology TopologyView `json:"topology"`

	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ActivatedAt *time.Time `json:"activated_at,omitempty"`
	ClosedAt    *time.Time `json:"closed_at,omitempty"`
}

// TopologyView is the immutable topology evidence captured for a window.
type TopologyView struct {
	SpanID      string `json:"span_id"`
	Generation  uint64 `json:"generation"`
	ActivePath  string `json:"active_path"`
	PrimaryOpen bool   `json:"primary_open"`
	ProtectOpen bool   `json:"protection_open"`
}

// Line identifies the physical resource a window occupies.
func (w Window) Line() string {
	parts := make([]string, 0, 3)
	for _, part := range []string{w.StationID, w.CircuitID, w.SpanID} {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, "/")
}

// overlaps reports whether two half-open time ranges intersect.
func overlaps(startA, endA, startB, endB time.Time) bool {
	return startA.Before(endB) && startB.Before(endA)
}

// validateSchedule checks the structural validity of a scheduling request.
func validateSchedule(spanID, stationID, circuitID string, start, end time.Time) error {
	if spanID == "" || stationID == "" || circuitID == "" {
		return errors.New("span, station and circuit are required for a maintenance window")
	}
	if start.IsZero() || end.IsZero() {
		return errors.New("window start and end times are required")
	}
	if !end.After(start) {
		return errors.New("window end must be after its start")
	}
	return nil
}
