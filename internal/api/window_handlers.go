package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/window"
)

func (s *Server) scheduleWindow(writer http.ResponseWriter, request *http.Request) {
	var input WindowScheduleRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	plan := window.Plan{
		SpanID: input.SpanID, StationID: input.StationID, CircuitID: input.CircuitID,
		PermitID: input.PermitID, StartAt: input.StartAt, EndAt: input.EndAt,
		Operator: input.Operator, Notes: input.Notes,
	}
	created, err := s.services.Windows.Schedule(plan)
	if err != nil {
		s.writeWindowError(writer, err)
		return
	}
	if err := s.services.Persist("maintenance-window", created.ID, created); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusCreated, created)
}

func (s *Server) preflightWindow(writer http.ResponseWriter, request *http.Request) {
	var input WindowScheduleRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	plan := window.Plan{
		SpanID: input.SpanID, StationID: input.StationID, CircuitID: input.CircuitID,
		PermitID: input.PermitID, StartAt: input.StartAt, EndAt: input.EndAt,
		Operator: input.Operator, Notes: input.Notes,
	}
	writeJSON(writer, http.StatusOK, WindowBlockersResponse{Blockers: s.services.Windows.Preflight(plan)})
}

func (s *Server) listWindows(writer http.ResponseWriter, request *http.Request) {
	var items []window.Window
	switch request.URL.Query().Get("state") {
	case "open":
		items = s.services.Windows.Open()
	case "history", "closed":
		items = s.services.Windows.History()
	default:
		items = s.services.Windows.List()
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) getWindow(writer http.ResponseWriter, request *http.Request) {
	item, exists := s.services.Windows.Get(chi.URLParam(request, "id"))
	if !exists {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "maintenance window not found"})
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func (s *Server) startWindow(writer http.ResponseWriter, request *http.Request) {
	item, err := s.services.Windows.Start(chi.URLParam(request, "id"))
	if err != nil {
		s.writeWindowError(writer, err)
		return
	}
	if err := s.services.Persist("maintenance-window-start", item.ID, item); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func (s *Server) completeWindow(writer http.ResponseWriter, request *http.Request) {
	var input WindowReceiptRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	receipt := window.CompletionReceipt{
		ID: input.ID, OTDRTraceID: input.OTDRTraceID,
		Operator: input.Operator, Notes: input.Notes,
		RecordedAt: s.services.Clock.Now().UTC(),
	}
	item, err := s.services.Windows.Complete(chi.URLParam(request, "id"), receipt)
	if err != nil {
		s.writeWindowError(writer, err)
		return
	}
	if err := s.services.Persist("maintenance-window-complete", item.ID, item); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func (s *Server) abandonWindow(writer http.ResponseWriter, request *http.Request) {
	var input WindowAbandonRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	item, err := s.services.Windows.Abandon(chi.URLParam(request, "id"), input.Reason)
	if err != nil {
		s.writeWindowError(writer, err)
		return
	}
	if err := s.services.Persist("maintenance-window-abandon", item.ID, item); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

// writeWindowError maps a window conflict to 409 with the blocker list and a
// missing window to 404; other validation failures become 400.
func (s *Server) writeWindowError(writer http.ResponseWriter, err error) {
	var conflictErr *window.ConflictError
	if errors.As(err, &conflictErr) {
		writeJSON(writer, http.StatusConflict, WindowBlockersResponse{Blockers: conflictErr.Blockers})
		return
	}
	if strings.Contains(err.Error(), "not found") {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeError(writer, http.StatusBadRequest, err)
}
