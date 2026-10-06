package api

import (
	"time"

	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/window"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type HealthResponse struct {
	Status    string    `json:"status"`
	Service   string    `json:"service"`
	Timestamp time.Time `json:"timestamp"`
}

type ScanRequest struct {
	StationID string  `json:"station_id"`
	LengthKM  float64 `json:"length_km"`
	Chunks    int     `json:"chunks"`
}

type SwitchoverRequest struct {
	Reason string `json:"reason"`
}

type IsolationRequest struct {
	CircuitID string `json:"circuit_id"`
}

type ReceiptRequest struct {
	CircuitID   string `json:"circuit_id"`
	OperationID string `json:"operation_id"`
	Isolated    bool   `json:"isolated"`
}

type AcceptanceRequest struct {
	AcceptanceID string         `json:"acceptance_id"`
	Direction    otdr.Direction `json:"direction"`
	Passed       bool           `json:"passed"`
	LossDB       float64        `json:"loss_db"`
	Message      string         `json:"message"`
}

type TelemetryRequest struct {
	Kind       string  `json:"kind"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit"`
	Sequence   uint64  `json:"sequence"`
	Generation uint64  `json:"generation"`
}

type RampRequest struct {
	Step         int     `json:"step"`
	Voltage      float64 `json:"voltage"`
	SamplesAfter uint64  `json:"samples_after"`
}

type ResetRequest struct {
	Generation uint64 `json:"generation"`
}

type AlarmRequest struct {
	Kind string `json:"kind"`
}

type TestRequest struct {
	Action string `json:"action"`
}

type IncidentRequest struct {
	Action       string  `json:"action"`
	StationID    string  `json:"station_id"`
	TraceID      string  `json:"trace_id"`
	DistanceKM   float64 `json:"distance_km"`
	RequiresPeer bool    `json:"requires_peer"`
}

type WindowScheduleRequest struct {
	SpanID    string    `json:"span_id"`
	StationID string    `json:"station_id"`
	CircuitID string    `json:"circuit_id"`
	PermitID  string    `json:"permit_id"`
	StartAt   time.Time `json:"start_at"`
	EndAt     time.Time `json:"end_at"`
	Operator  string    `json:"operator"`
	Notes     string    `json:"notes"`
}

type WindowReceiptRequest struct {
	ID          string `json:"id"`
	OTDRTraceID string `json:"otdr_trace_id"`
	Operator    string `json:"operator"`
	Notes       string `json:"notes"`
}

type WindowAbandonRequest struct {
	Reason string `json:"reason"`
}

type WindowBlockersResponse struct {
	Blockers []window.Blocker `json:"blockers"`
}
