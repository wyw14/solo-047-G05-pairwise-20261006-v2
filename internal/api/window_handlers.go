package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/window"
)

// writeGateError renders a gate failure with the occupying lines and reasons.
func writeGateError(writer http.ResponseWriter, status int, err *window.GateError) {
	writeJSON(writer, status, map[string]any{"error": err.Error(), "blocks": err.Blocks})
}

func (s *Server) scheduleWindow(writer http.ResponseWriter, request *http.Request) {
	var input WindowScheduleRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	win, err := s.services.Windows.Schedule(window.ScheduleRequest{
		SpanID: input.SpanID, StationID: input.StationID, CircuitID: input.CircuitID,
		PermitID: input.PermitID, StartsAt: input.StartsAt, EndsAt: input.EndsAt, Purpose: input.Purpose,
	})
	if err != nil {
		var gate *window.GateError
		if errors.As(err, &gate) {
			writeGateError(writer, http.StatusConflict, gate)
			return
		}
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if err := s.services.Persist("maintenance-window", win.ID, win); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusCreated, win)
}

func (s *Server) listWindows(writer http.ResponseWriter, request *http.Request) {
	var items []window.Window
	switch request.URL.Query().Get("view") {
	case "open":
		items = s.services.Windows.Open()
	case "history":
		items = s.services.Windows.History()
	default:
		items = s.services.Windows.List()
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) listOverdueWindows(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.Windows.Overdue()})
}

func (s *Server) getWindow(writer http.ResponseWriter, request *http.Request) {
	win, exists := s.services.Windows.Get(chi.URLParam(request, "id"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "maintenance window not found"})
		return
	}
	writeJSON(writer, http.StatusOK, win)
}

func (s *Server) activateWindow(writer http.ResponseWriter, request *http.Request) {
	win, gate := s.services.Windows.Activate(chi.URLParam(request, "id"))
	if gate != nil {
		if !existsWindow(gate) {
			writeError(writer, http.StatusNotFound, errors.New("maintenance window not found"))
			return
		}
		_ = s.services.Persist("maintenance-window-blocked", win.ID, win)
		writeGateError(writer, http.StatusConflict, gate)
		return
	}
	if err := s.services.Persist("maintenance-window-active", win.ID, win); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, win)
}

func existsWindow(gate *window.GateError) bool {
	for _, block := range gate.Blocks {
		if block.Reason == window.ReasonNotFound {
			return false
		}
	}
	return true
}

func (s *Server) completeWindow(writer http.ResponseWriter, request *http.Request) {
	win, err := s.services.Windows.Complete(chi.URLParam(request, "id"))
	if err != nil {
		var gate *window.GateError
		if errors.As(err, &gate) {
			writeGateError(writer, http.StatusConflict, gate)
			return
		}
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("maintenance-window-completed", win.ID, win); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, win)
}

func (s *Server) abandonWindow(writer http.ResponseWriter, request *http.Request) {
	var input WindowAbandonRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	win, err := s.services.Windows.Abandon(chi.URLParam(request, "id"), input.Reason)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("maintenance-window-abandoned", win.ID, win); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, win)
}

func (s *Server) rescheduleWindow(writer http.ResponseWriter, request *http.Request) {
	var input WindowRescheduleRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	win, err := s.services.Windows.Reschedule(chi.URLParam(request, "id"), input.StartsAt, input.EndsAt)
	if err != nil {
		var gate *window.GateError
		if errors.As(err, &gate) {
			writeGateError(writer, http.StatusConflict, gate)
			return
		}
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("maintenance-window-rescheduled", win.ID, win); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, win)
}

// attachWindowTrace binds an existing OTDR trace to an active window.
func (s *Server) attachWindowTrace(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "id")
	var input WindowTraceRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if input.TraceID == "" {
		writeError(writer, http.StatusBadRequest, errors.New("trace_id is required"))
		return
	}
	trace, exists := s.services.OTDR.Get(input.TraceID)
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "OTDR trace not found"})
		return
	}
	win, err := s.services.Windows.AttachTrace(id, trace.ID, trace.Generation)
	if err != nil {
		var gate *window.GateError
		if errors.As(err, &gate) {
			writeGateError(writer, http.StatusConflict, gate)
			return
		}
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("maintenance-window-trace", win.ID, win); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, win)
}

// runWindowScan executes a topology-pinned OTDR scan inside an active window
// and attaches the resulting trace, proving the test ran on the window's
// topology generation.
func (s *Server) runWindowScan(writer http.ResponseWriter, request *http.Request) {
	id := chi.URLParam(request, "id")
	win, exists := s.services.Windows.Get(id)
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "maintenance window not found"})
		return
	}
	var input ScanRequest
	if request.ContentLength > 0 {
		if err := decodeJSON(request, &input); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
	}
	if input.StationID == "" {
		input.StationID = win.StationID
	}
	target, err := s.services.Span(win.SpanID)
	if err != nil {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: err.Error()})
		return
	}
	if input.LengthKM <= 0 {
		input.LengthKM = target.Snapshot().LengthKM
	}
	if input.Chunks <= 0 {
		input.Chunks = 4
	}
	trace, err := s.services.OTDR.Run(request.Context(), win.SpanID, input.StationID, input.LengthKM, input.Chunks)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	win, err = s.services.Windows.AttachTrace(id, trace.ID, trace.Generation)
	if err != nil {
		var gate *window.GateError
		if errors.As(err, &gate) {
			writeGateError(writer, http.StatusConflict, gate)
			return
		}
		writeError(writer, http.StatusConflict, err)
		return
	}
	if err := s.services.Persist("maintenance-window-trace", win.ID, win); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"window": win, "trace": trace})
}
