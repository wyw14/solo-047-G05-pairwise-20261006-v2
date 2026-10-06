package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/topology"
	"github.com/wyw14/cry-103/internal/window"
)

func doJSON(t *testing.T, server *Server, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(method, path, reader))
	var decoded map[string]any
	if recorder.Body.Len() > 0 {
		if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("decode %s body: %v\n%s", path, err, recorder.Body.String())
		}
	}
	return recorder.Code, decoded
}

func TestMaintenanceWindowHTTPFlow(t *testing.T) {
	server := testServer(t)

	// 1. Request feed isolation for circuit B.
	status, body := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-isolations", map[string]any{"circuit_id": "B"})
	if status != http.StatusAccepted {
		t.Fatalf("isolation status=%d body=%v", status, body)
	}
	permitID, _ := body["id"].(string)
	operationID, _ := body["operation_id"].(string)

	// 2. Record the isolation receipt, evaluate and ground the permit.
	if status, body = doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts",
		map[string]any{"circuit_id": "B", "operation_id": operationID, "isolated": true}); status != http.StatusCreated {
		t.Fatalf("receipt status=%d body=%v", status, body)
	}
	if status, _ = doJSON(t, server, http.MethodPost, "/api/permits/"+permitID+"/evaluate", nil); status != http.StatusOK {
		t.Fatalf("evaluate status=%d", status)
	}
	if status, body = doJSON(t, server, http.MethodPost, "/api/permits/"+permitID+"/ground", nil); status != http.StatusOK {
		t.Fatalf("ground status=%d body=%v", status, body)
	}

	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	scheduleBody := map[string]any{
		"span_id": "S3", "station_id": "east", "circuit_id": "B", "permit_id": permitID,
		"starts_at": start, "ends_at": start.Add(2 * time.Hour), "purpose": "S3 splice",
	}
	status, body = doJSON(t, server, http.MethodPost, "/api/maintenance-windows", scheduleBody)
	if status != http.StatusCreated {
		t.Fatalf("schedule status=%d body=%v", status, body)
	}
	windowID, _ := body["id"].(string)
	if gen := body["topology"].(map[string]any)["generation"].(float64); gen != 1 {
		t.Fatalf("window must freeze topology generation 1, got %v", gen)
	}

	// 3. An overlapping window on the same circuit is rejected with the line and reason.
	overlap := scheduleBody
	overlap["starts_at"] = start.Add(90 * time.Minute)
	overlap["ends_at"] = start.Add(3 * time.Hour)
	status, body = doJSON(t, server, http.MethodPost, "/api/maintenance-windows", overlap)
	if status != http.StatusConflict {
		t.Fatalf("overlap expected 409, got %d body=%v", status, body)
	}
	blocks, _ := body["blocks"].([]any)
	if len(blocks) != 1 {
		t.Fatalf("expected one block, got %v", blocks)
	}
	first := blocks[0].(map[string]any)
	if first["reason"] != string(window.ReasonOverlapSameCircuit) || first["line"] != "east/B/S3" {
		t.Fatalf("block must identify occupied line and reason: %v", first)
	}
	if first["window_id"] != windowID {
		t.Fatalf("block must name the occupying window: %v", first)
	}

	// 4. Activate the window.
	if status, body = doJSON(t, server, http.MethodPost, "/api/maintenance-windows/"+windowID+"/activate", nil); status != http.StatusOK {
		t.Fatalf("activate status=%d body=%v", status, body)
	}
	if body["state"] != string(window.StateActive) {
		t.Fatalf("window not active: %v", body["state"])
	}

	// 5. Run a topology-pinned OTDR scan inside the window.
	status, body = doJSON(t, server, http.MethodPost,
		fmt.Sprintf("/api/maintenance-windows/%s/otdr-scans", windowID), map[string]any{"chunks": 2})
	if status != http.StatusCreated {
		t.Fatalf("window scan status=%d body=%v", status, body)
	}
	winBody := body["window"].(map[string]any)
	if winBody["otdr_trace_id"] == "" {
		t.Fatal("scan did not attach its trace to the window")
	}

	// 6. Completion requires the post-work receipt; it fails before the hand-back.
	if status, body = doJSON(t, server, http.MethodPost, "/api/maintenance-windows/"+windowID+"/complete", nil); status != http.StatusConflict {
		t.Fatalf("complete without receipt expected 409, got %d body=%v", status, body)
	}
	blocks, _ = body["blocks"].([]any)
	if blocks[0].(map[string]any)["reason"] != string(window.ReasonClosureMissing) {
		t.Fatalf("expected closure-evidence-missing: %v", blocks)
	}

	// 7. Record the hand-back receipt after the clock moves on, and complete.
	server.services.Clock.(*mutableClock).advance(time.Minute)
	if status, _ = doJSON(t, server, http.MethodPost, "/api/landings/east/feed-receipts",
		map[string]any{"circuit_id": "B", "operation_id": operationID, "isolated": true}); status != http.StatusCreated {
		t.Fatalf("hand-back receipt status=%d", status)
	}
	if status, body = doJSON(t, server, http.MethodPost, "/api/maintenance-windows/"+windowID+"/complete", nil); status != http.StatusOK {
		t.Fatalf("complete status=%d body=%v", status, body)
	}
	if body["state"] != string(window.StateCompleted) {
		t.Fatalf("window not completed: %v", body["state"])
	}
	closure := body["closure"].(map[string]any)
	if closure["abandoned"] != false || closure["operation"] != operationID {
		t.Fatalf("closure must reference the receipt operation: %v", closure)
	}

	// 8. The live topology advances; the historical window keeps its generation.
	registry := server.services.Topology
	if _, err := registry.Open("S3", topology.PathPrimary, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Connect("S3", topology.PathProtection, time.Now()); err != nil {
		t.Fatal(err)
	}
	status, body = doJSON(t, server, http.MethodGet, "/api/maintenance-windows?view=history", nil)
	if status != http.StatusOK {
		t.Fatalf("history status=%d", status)
	}
	items, _ := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected one historical window, got %d", len(items))
	}
	historical := items[0].(map[string]any)
	if gen := historical["topology"].(map[string]any)["generation"].(float64); gen != 1 {
		t.Fatalf("historical window must retain generation 1, got %v", gen)
	}
	if historical["state"] != string(window.StateCompleted) {
		t.Fatalf("historical window state wrong: %v", historical["state"])
	}
}

func TestAbandonWindowWithoutReceipt(t *testing.T) {
	server := testServer(t)
	status, body := doJSON(t, server, http.MethodPost, "/api/landings/east/feed-isolations", map[string]any{"circuit_id": "B"})
	if status != http.StatusAccepted {
		t.Fatalf("isolation status=%d", status)
	}
	permitID, _ := body["id"].(string)
	operationID, _ := body["operation_id"].(string)

	// A scheduled, never-activated window is waived explicitly.
	start := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC)
	status, body = doJSON(t, server, http.MethodPost, "/api/maintenance-windows", map[string]any{
		"span_id": "S3", "station_id": "east", "circuit_id": "B", "permit_id": permitID,
		"starts_at": start, "ends_at": start.Add(time.Hour), "purpose": "standby",
	})
	if status != http.StatusCreated {
		t.Fatalf("schedule status=%d body=%v", status, body)
	}
	windowID, _ := body["id"].(string)
	status, body = doJSON(t, server, http.MethodPost, "/api/maintenance-windows/"+windowID+"/abandon",
		map[string]any{"reason": "cancelled by NOC"})
	if status != http.StatusOK {
		t.Fatalf("abandon status=%d body=%v", status, body)
	}
	if body["state"] != string(window.StateAbandoned) {
		t.Fatalf("state=%v", body["state"])
	}
	closure := body["closure"].(map[string]any)
	if closure["abandoned"] != true || closure["reason"] != "cancelled by NOC" {
		t.Fatalf("abandon closure wrong: %v", closure)
	}
	_ = operationID
}
