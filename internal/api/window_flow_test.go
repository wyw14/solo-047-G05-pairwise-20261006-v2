package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/topology"
	"github.com/wyw14/cry-103/internal/window"
)

// groundedPermitViaHTTP drives the feed-isolation workflow through the HTTP
// API and returns the grounded permit.
func groundedPermitViaHTTP(t *testing.T, server *Server) repair.Permit {
	t.Helper()
	var permit repair.Permit

	isolation := httptest.NewRecorder()
	body, _ := json.Marshal(IsolationRequest{CircuitID: "B"})
	server.Handler().ServeHTTP(isolation, httptest.NewRequest(http.MethodPost, "/api/landings/east/feed-isolations", bytes.NewReader(body)))
	if isolation.Code != http.StatusAccepted {
		t.Fatalf("isolation status=%d body=%s", isolation.Code, isolation.Body.String())
	}
	if err := json.Unmarshal(isolation.Body.Bytes(), &permit); err != nil {
		t.Fatal(err)
	}

	receipt, _ := json.Marshal(ReceiptRequest{CircuitID: "B", OperationID: permit.OperationID, Isolated: true})
	recorded := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorded, httptest.NewRequest(http.MethodPost, "/api/landings/east/feed-receipts", bytes.NewReader(receipt)))
	if recorded.Code != http.StatusCreated {
		t.Fatalf("receipt status=%d body=%s", recorded.Code, recorded.Body.String())
	}

	evaluated := httptest.NewRecorder()
	server.Handler().ServeHTTP(evaluated, httptest.NewRequest(http.MethodPost, "/api/permits/"+permit.ID+"/evaluate", nil))
	if evaluated.Code != http.StatusOK {
		t.Fatalf("evaluate status=%d body=%s", evaluated.Code, evaluated.Body.String())
	}
	grounded := httptest.NewRecorder()
	server.Handler().ServeHTTP(grounded, httptest.NewRequest(http.MethodPost, "/api/permits/"+permit.ID+"/ground", nil))
	if grounded.Code != http.StatusOK {
		t.Fatalf("ground status=%d body=%s", grounded.Code, grounded.Body.String())
	}
	if err := json.Unmarshal(grounded.Body.Bytes(), &permit); err != nil {
		t.Fatal(err)
	}
	return permit
}

func serveJSON(server *Server, method, path string, payload any) *httptest.ResponseRecorder {
	var reader *bytes.Reader
	if payload != nil {
		body, _ := json.Marshal(payload)
		reader = bytes.NewReader(body)
	} else {
		reader = bytes.NewReader(nil)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(method, path, reader))
	return recorder
}

func TestMaintenanceWindowLifecycleHTTP(t *testing.T) {
	server := testServer(t)
	fixedNow := time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)
	permit := groundedPermitViaHTTP(t, server)

	plan := WindowScheduleRequest{
		SpanID: "S3", StationID: "east", CircuitID: "B", PermitID: permit.ID,
		StartAt: fixedNow, EndAt: fixedNow.Add(time.Hour), Operator: "rig-7", Notes: "splice",
	}
	scheduled := serveJSON(server, http.MethodPost, "/api/maintenance-windows", plan)
	if scheduled.Code != http.StatusCreated {
		t.Fatalf("schedule status=%d body=%s", scheduled.Code, scheduled.Body.String())
	}
	var win window.Window
	if err := json.Unmarshal(scheduled.Body.Bytes(), &win); err != nil {
		t.Fatal(err)
	}
	if win.State != window.StateScheduled || win.Topology.Generation != 1 {
		t.Fatalf("scheduled window=%+v", win)
	}

	// An overlapping plan is rejected with the occupied lines and reasons.
	overlap := serveJSON(server, http.MethodPost, "/api/maintenance-windows", plan)
	if overlap.Code != http.StatusConflict {
		t.Fatalf("overlap status=%d body=%s", overlap.Code, overlap.Body.String())
	}
	var blockers WindowBlockersResponse
	if err := json.Unmarshal(overlap.Body.Bytes(), &blockers); err != nil {
		t.Fatal(err)
	}
	lines := map[window.ResourceKind]string{}
	for _, blocker := range blockers.Blockers {
		if blocker.Code != window.BlockedWindowOverlap || blocker.WindowID != win.ID {
			t.Fatalf("blocker=%+v", blocker)
		}
		lines[blocker.Kind] = blocker.Line
	}
	if len(lines) != 3 || lines[window.ResourceStation] != "east" ||
		lines[window.ResourceCircuit] != "east/B" || lines[window.ResourceSpan] != "S3" {
		t.Fatalf("overlap lines=%+v", lines)
	}

	// Start: station online, circuit grounded, permit grounded, interval current.
	started := serveJSON(server, http.MethodPost, "/api/maintenance-windows/"+win.ID+"/start", nil)
	if started.Code != http.StatusOK {
		t.Fatalf("start status=%d body=%s", started.Code, started.Body.String())
	}

	// Closing without an OTDR-referencing receipt fails; abandonment stays explicit.
	noTrace := serveJSON(server, http.MethodPost, "/api/maintenance-windows/"+win.ID+"/complete",
		WindowReceiptRequest{ID: "rcpt-1"})
	if noTrace.Code != http.StatusBadRequest {
		t.Fatalf("bad-receipt status=%d body=%s", noTrace.Code, noTrace.Body.String())
	}
	completed := serveJSON(server, http.MethodPost, "/api/maintenance-windows/"+win.ID+"/complete",
		WindowReceiptRequest{ID: "rcpt-1", OTDRTraceID: "scan-77", Operator: "rig-7"})
	if completed.Code != http.StatusOK {
		t.Fatalf("complete status=%d body=%s", completed.Code, completed.Body.String())
	}
	if err := json.Unmarshal(completed.Body.Bytes(), &win); err != nil {
		t.Fatal(err)
	}
	if win.State != window.StateCompleted || win.Receipt == nil || win.Receipt.OTDRTraceID != "scan-77" {
		t.Fatalf("completed window=%+v", win)
	}

	// History keeps the pinned generation even after the live topology moves.
	if _, err := server.services.Topology.Switch("S3", topology.PathProtection, fixedNow); err != nil {
		t.Fatal(err)
	}
	historyRecorder := serveJSON(server, http.MethodGet, "/api/maintenance-windows?state=history", nil)
	var history struct {
		Items []window.Window `json:"items"`
	}
	if err := json.Unmarshal(historyRecorder.Body.Bytes(), &history); err != nil {
		t.Fatal(err)
	}
	if len(history.Items) != 1 || history.Items[0].Topology.Generation != 1 || history.Items[0].Receipt == nil {
		t.Fatalf("history=%+v", history.Items)
	}

	// The completion was appended to the event journal.
	page := serveJSON(server, http.MethodGet, "/api/events?kind=maintenance-window-complete", nil)
	if page.Code != http.StatusOK || !bytes.Contains(page.Body.Bytes(), []byte(win.ID)) {
		t.Fatalf("journal page=%s", page.Body.String())
	}
}

func TestMaintenanceWindowGatingAndAbandonHTTP(t *testing.T) {
	server := testServer(t)
	fixedNow := time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)

	// Request isolation but do not ground: planning ahead still succeeds, while
	// preflight/start report the outstanding isolation work.
	isolation := serveJSON(server, http.MethodPost, "/api/landings/east/feed-isolations", IsolationRequest{CircuitID: "B"})
	var permit repair.Permit
	if err := json.Unmarshal(isolation.Body.Bytes(), &permit); err != nil {
		t.Fatal(err)
	}
	plan := WindowScheduleRequest{
		SpanID: "S3", StationID: "east", CircuitID: "B", PermitID: permit.ID,
		StartAt: fixedNow, EndAt: fixedNow.Add(time.Hour),
	}
	preflight := serveJSON(server, http.MethodPost, "/api/maintenance-windows/preflight", plan)
	var blockers WindowBlockersResponse
	if err := json.Unmarshal(preflight.Body.Bytes(), &blockers); err != nil {
		t.Fatal(err)
	}
	counts := map[window.BlockerCode]int{}
	for _, blocker := range blockers.Blockers {
		counts[blocker.Code]++
	}
	if counts[window.BlockedCircuitNotGrounded] != 1 || counts[window.BlockedPermitNotReady] != 1 {
		t.Fatalf("preflight blockers=%+v", blockers.Blockers)
	}

	scheduled := serveJSON(server, http.MethodPost, "/api/maintenance-windows", plan)
	if scheduled.Code != http.StatusCreated {
		t.Fatalf("schedule ahead status=%d body=%s", scheduled.Code, scheduled.Body.String())
	}
	var win window.Window
	if err := json.Unmarshal(scheduled.Body.Bytes(), &win); err != nil {
		t.Fatal(err)
	}
	started := serveJSON(server, http.MethodPost, "/api/maintenance-windows/"+win.ID+"/start", nil)
	if started.Code != http.StatusConflict || !bytes.Contains(started.Body.Bytes(), []byte(window.BlockedCircuitNotGrounded)) {
		t.Fatalf("start status=%d body=%s", started.Code, started.Body.String())
	}

	// Abandon needs a reason; once abandoned the slot frees up for re-planning.
	noReason := serveJSON(server, http.MethodPost, "/api/maintenance-windows/"+win.ID+"/abandon", WindowAbandonRequest{})
	if noReason.Code != http.StatusBadRequest {
		t.Fatalf("abandon-no-reason status=%d", noReason.Code)
	}
	abandoned := serveJSON(server, http.MethodPost, "/api/maintenance-windows/"+win.ID+"/abandon",
		WindowAbandonRequest{Reason: "vessel weather hold"})
	if abandoned.Code != http.StatusOK {
		t.Fatalf("abandon status=%d body=%s", abandoned.Code, abandoned.Body.String())
	}
	replanned := serveJSON(server, http.MethodPost, "/api/maintenance-windows", plan)
	if replanned.Code != http.StatusCreated {
		t.Fatalf("re-plan status=%d body=%s", replanned.Code, replanned.Body.String())
	}
}
