package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCoordinator_API_CRUD(t *testing.T) {
	c := NewCoordinator(Config{VNodesPerNode: 64, Replicas: 3}, nil)
	handler := c.Routes()

	// 1. Check Stats initially
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/stats returned code %d", rec.Code)
	}

	// 2. Check Ring initially empty
	req = httptest.NewRequest(http.MethodGet, "/api/v1/ring?key=mykey", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/ring returned code %d", rec.Code)
	}

	// 3. Test Console error when no nodes
	body, _ := json.Marshal(map[string]string{"cmd": "GET testkey"})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/console", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when querying empty ring, got %d", rec.Code)
	}

	// 4. Test Benchmark Run
	benchBody, _ := json.Marshal(map[string]interface{}{
		"concurrency":  5,
		"duration_sec": 1,
		"read_ratio":   0.8,
	})
	req = httptest.NewRequest(http.MethodPost, "/api/v1/bench/run", bytes.NewReader(benchBody))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/bench/run code %d", rec.Code)
	}
	var res BenchmarkRun
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal bench result: %v", err)
	}
	if res.Concurrency != 5 {
		t.Fatalf("expected concurrency 5, got %d", res.Concurrency)
	}
}
