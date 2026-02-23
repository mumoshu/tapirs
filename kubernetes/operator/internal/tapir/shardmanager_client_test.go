package tapir

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestShardManagerClient_RegisterShard(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/register" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.Error(w, "not found", 404)
			return
		}
		if r.Method != http.MethodPost {
			t.Errorf("unexpected method: %s", r.Method)
			http.Error(w, "method not allowed", 405)
			return
		}

		body, _ := io.ReadAll(r.Body)
		var req registerRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("bad request body: %v", err)
			http.Error(w, "bad request", 400)
			return
		}

		if req.Shard != 0 {
			t.Errorf("expected shard 0, got %d", req.Shard)
		}
		if req.KeyRangeStart != nil {
			t.Errorf("expected nil key_range_start, got %q", *req.KeyRangeStart)
		}
		if req.KeyRangeEnd == nil || *req.KeyRangeEnd != "n" {
			t.Errorf("expected key_range_end 'n', got %v", req.KeyRangeEnd)
		}
		if len(req.Replicas) != 3 {
			t.Errorf("expected 3 replicas, got %d", len(req.Replicas))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(shardManagerResponse{OK: true})
	}))
	defer srv.Close()

	client := &ShardManagerClient{BaseURL: srv.URL}
	err := client.RegisterShard(context.Background(), 0, "", "n",
		[]string{"10.0.0.1:6000", "10.0.0.2:6000", "10.0.0.3:6000"})
	if err != nil {
		t.Fatalf("RegisterShard() error: %v", err)
	}
}

func TestShardManagerClient_RegisterShard_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(shardManagerResponse{Error: "shard 0 not found in discovery"})
	}))
	defer srv.Close()

	client := &ShardManagerClient{BaseURL: srv.URL}
	err := client.RegisterShard(context.Background(), 0, "", "n", nil)
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
}

func TestShardManagerClient_Split(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/split" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var req splitRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		if req.Source != 0 || req.SplitKey != "m" || req.NewShard != 2 {
			t.Errorf("unexpected split request: %+v", req)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(shardManagerResponse{OK: true})
	}))
	defer srv.Close()

	client := &ShardManagerClient{BaseURL: srv.URL}
	err := client.Split(context.Background(), 0, "m", 2, []string{"10.0.0.4:6002"})
	if err != nil {
		t.Fatalf("Split() error: %v", err)
	}
}

func TestShardManagerClient_Merge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/merge" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		var req mergeRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("bad request body: %v", err)
		}
		if req.Absorbed != 1 || req.Surviving != 0 {
			t.Errorf("unexpected merge request: %+v", req)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(shardManagerResponse{OK: true})
	}))
	defer srv.Close()

	client := &ShardManagerClient{BaseURL: srv.URL}
	err := client.Merge(context.Background(), 1, 0)
	if err != nil {
		t.Fatalf("Merge() error: %v", err)
	}
}
