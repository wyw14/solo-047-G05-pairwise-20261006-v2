package window

import (
	"errors"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
)

// State is the lifecycle state of a maintenance window.
type State string

const (
	StateScheduled State = "scheduled"
	StateActive    State = "active"
	StateCompleted State = "completed"
	StateAbandoned State = "abandoned"
)

// ResourceKind names the type of resource a window exclusively occupies.
type ResourceKind string

const (
	ResourceStation ResourceKind = "station"
	ResourceCircuit ResourceKind = "circuit"
	ResourceSpan    ResourceKind = "span"
)

// BlockerCode classifies why a window cannot be scheduled or started.
type BlockerCode string

const (
	// BlockedWindowOverlap: another open window occupies the same resource at the same time.
	BlockedWindowOverlap BlockerCode = "window-overlap"
	// BlockedStationOffline: the landing station is not reporting online.
	BlockedStationOffline BlockerCode = "station-offline"
	// BlockedPermitNotReady: the repair permit has not reached the grounded state.
	BlockedPermitNotReady BlockerCode = "permit-not-grounded"
	// BlockedCircuitNotGrounded: the feed circuit is not grounded for this window.
	BlockedCircuitNotGrounded BlockerCode = "circuit-not-grounded"
	// BlockedTopologyMissing: the span has no registered cross-connect topology.
	BlockedTopologyMissing BlockerCode = "topology-missing"
	// BlockedTopologyChanged: the live topology generation advanced past the generation pinned by the window.
	BlockedTopologyChanged BlockerCode = "topology-changed"
	// BlockedInvalidWindow: the requested window itself is malformed.
	BlockedInvalidWindow BlockerCode = "invalid-window"
	// BlockedStateConflict: the window is not in the state required for the action.
	BlockedStateConflict BlockerCode = "state-conflict"
)

// Blocker describes one reason a schedule/start attempt is rejected. When the
// reason is an overlapping window, Line identifies the occupied line.
type Blocker struct {
	Code     BlockerCode  `json:"code"`
	Kind     ResourceKind `json:"kind"`
	Line     string       `json:"line,omitempty"`
	WindowID string       `json:"window_id,omitempty"`
	Reason   string       `json:"reason"`
	Detail   string       `json:"detail,omitempty"`
}

// TopologyGeneration is the immutable topology view captured when a window is
// scheduled. It is preserved on history windows so closed records keep showing
// the topology generation that was in force when the work was planned.
type TopologyGeneration struct {
	SpanID      string        `json:"span_id"`
	Generation  uint64        `json:"generation"`
	ActivePath  topology.Path `json:"active_path"`
	PrimaryOpen bool          `json:"primary_open"`
	ProtectOpen bool          `json:"protection_open"`
	CapturedAt  time.Time     `json:"captured_at"`
}

// CompletionReceipt is the proof that must be attached before a window closes.
// Repair work cannot be "completed" without one; windows without a receipt can
// only be closed by explicit abandonment.
type CompletionReceipt struct {
	ID          string    `json:"id"`
	OTDRTraceID string    `json:"otdr_trace_id"`
	PermitID    string    `json:"permit_id"`
	StationID   string    `json:"station_id"`
	Operator    string    `json:"operator"`
	Notes       string    `json:"notes"`
	RecordedAt  time.Time `json:"recorded_at"`
}

// Plan is the request to schedule a maintenance window.
type Plan struct {
	SpanID    string
	StationID string
	CircuitID string
	PermitID  string
	StartAt   time.Time
	EndAt     time.Time
	Operator  string
	Notes     string
}

func (p Plan) validate(now time.Time) error {
	if p.SpanID == "" {
		return errors.New("window span is required")
	}
	if p.StationID == "" {
		return errors.New("window station is required")
	}
	if p.CircuitID == "" {
		return errors.New("window feed circuit is required")
	}
	if p.PermitID == "" {
		return errors.New("window repair permit is required")
	}
	if !p.EndAt.After(p.StartAt) {
		return errors.New("window end must be after start")
	}
	if p.StartAt.Before(now) {
		return errors.New("window start is in the past")
	}
	return nil
}

// Window is the stored, JSON-serializable window record.
type Window struct {
	ID            string             `json:"id"`
	SpanID        string             `json:"span_id"`
	StationID     string             `json:"station_id"`
	CircuitID     string             `json:"circuit_id"`
	PermitID      string             `json:"permit_id"`
	Operator      string             `json:"operator"`
	Notes         string             `json:"notes"`
	State         State              `json:"state"`
	StartAt       time.Time          `json:"start_at"`
	EndAt         time.Time          `json:"end_at"`
	Topology      TopologyGeneration `json:"topology"`
	Receipt       *CompletionReceipt `json:"receipt,omitempty"`
	AbandonReason string             `json:"abandon_reason,omitempty"`
	CreatedAt     time.Time          `json:"created_at"`
	UpdatedAt     time.Time          `json:"updated_at"`
	StartedAt     time.Time          `json:"started_at,omitempty"`
	ClosedAt      time.Time          `json:"closed_at,omitempty"`
}

// terminal reports whether the window can no longer change.
func (w Window) terminal() bool {
	return w.State == StateCompleted || w.State == StateAbandoned
}

// open reports whether the window still occupies its resources on the timeline.
func (w Window) open() bool {
	return w.State == StateScheduled || w.State == StateActive
}

// overlaps reports half-open time intersection: [StartAt, EndAt).
func (w Window) overlaps(start, end time.Time) bool {
	return w.StartAt.Before(end) && start.Before(w.EndAt)
}

// resourceKey is the identity of an exclusively occupied line.
func resourceKey(kind ResourceKind, line string) string {
	return string(kind) + "\x00" + line
}

func stationLine(stationID string) string { return stationID }
func circuitLine(stationID, circuitID string) string {
	return stationID + "/" + circuitID
}
func spanLine(spanID string) string { return spanID }

// occupiedLines returns every line the window holds for the duration of its run.
func (w Window) occupiedLines() map[string]ResourceKind {
	return map[string]ResourceKind{
		resourceKey(ResourceStation, stationLine(w.StationID)):              ResourceStation,
		resourceKey(ResourceCircuit, circuitLine(w.StationID, w.CircuitID)): ResourceCircuit,
		resourceKey(ResourceSpan, spanLine(w.SpanID)):                       ResourceSpan,
	}
}
