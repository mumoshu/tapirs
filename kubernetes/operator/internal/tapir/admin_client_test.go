package tapir

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// mockAdminServer accepts one TCP connection, reads a JSON request, and
// replies with the provided response.
func mockAdminServer(t *testing.T, handler func(req adminRequest) AdminResponse) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go func() {
				defer func() { _ = conn.Close() }()
				scanner := bufio.NewScanner(conn)
				if !scanner.Scan() {
					return
				}
				var req adminRequest
				if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
					resp := AdminResponse{OK: false, Message: fmt.Sprintf("bad request: %v", err)}
					data, _ := json.Marshal(resp)
					_, _ = conn.Write(append(data, '\n'))
					return
				}
				resp := handler(req)
				data, _ := json.Marshal(resp)
				_, _ = conn.Write(append(data, '\n'))
			}()
		}
	}()
	return ln.Addr().String()
}

func TestAdminClient_Status(t *testing.T) {
	addr := mockAdminServer(t, func(req adminRequest) AdminResponse {
		if req.Command != "status" {
			return AdminResponse{OK: false, Message: "unexpected command: " + req.Command}
		}
		return AdminResponse{
			OK:      true,
			Message: "2 replica(s) running",
			Shards: []ShardInfo{
				{Shard: 0, ListenAddr: "10.0.0.1:6000"},
				{Shard: 1, ListenAddr: "10.0.0.1:6001"},
			},
		}
	})

	client := &AdminClient{Addr: addr, Timeout: 2 * time.Second}
	resp, err := client.Status(context.Background())
	if err != nil {
		t.Fatalf("Status() error: %v", err)
	}
	if !resp.OK {
		t.Fatalf("Status() not ok: %s", resp.Message)
	}
	if len(resp.Shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(resp.Shards))
	}
	if resp.Shards[0].Shard != 0 || resp.Shards[0].ListenAddr != "10.0.0.1:6000" {
		t.Errorf("unexpected shard[0]: %+v", resp.Shards[0])
	}
}

func TestAdminClient_AddReplica_WithMembership(t *testing.T) {
	addr := mockAdminServer(t, func(req adminRequest) AdminResponse {
		if req.Command != "add_replica" {
			return AdminResponse{OK: false, Message: "unexpected command"}
		}
		if req.Shard == nil || *req.Shard != 0 {
			return AdminResponse{OK: false, Message: "bad shard"}
		}
		if req.ListenAddr != "10.0.0.1:6000" {
			return AdminResponse{OK: false, Message: "bad listen_addr"}
		}
		if req.Storage != "memory" {
			return AdminResponse{OK: false, Message: "bad storage"}
		}
		if len(req.Membership) != 3 {
			return AdminResponse{OK: false, Message: fmt.Sprintf("expected 3 membership, got %d", len(req.Membership))}
		}
		return AdminResponse{OK: true, Message: "replica for shard 0 created"}
	})

	client := &AdminClient{Addr: addr, Timeout: 2 * time.Second}
	err := client.AddReplica(context.Background(), 0, "10.0.0.1:6000",
		[]string{"10.0.0.1:6000", "10.0.0.2:6000", "10.0.0.3:6000"}, "memory")
	if err != nil {
		t.Fatalf("AddReplica() error: %v", err)
	}
}

func TestAdminClient_AddReplica_WithoutMembership(t *testing.T) {
	addr := mockAdminServer(t, func(req adminRequest) AdminResponse {
		if req.Command != "add_replica" {
			return AdminResponse{OK: false, Message: "unexpected command"}
		}
		if req.Membership != nil {
			return AdminResponse{OK: false, Message: "expected nil membership for dynamic join"}
		}
		return AdminResponse{OK: true, Message: "replica for shard 0 created"}
	})

	client := &AdminClient{Addr: addr, Timeout: 2 * time.Second}
	err := client.AddReplica(context.Background(), 0, "10.0.0.1:6000", nil, "memory")
	if err != nil {
		t.Fatalf("AddReplica() error: %v", err)
	}
}

func TestAdminClient_RemoveReplica(t *testing.T) {
	addr := mockAdminServer(t, func(req adminRequest) AdminResponse {
		if req.Command != "remove_replica" {
			return AdminResponse{OK: false, Message: "unexpected command"}
		}
		if req.Shard == nil || *req.Shard != 1 {
			return AdminResponse{OK: false, Message: "bad shard"}
		}
		return AdminResponse{OK: true, Message: "replica for shard 1 removed"}
	})

	client := &AdminClient{Addr: addr, Timeout: 2 * time.Second}
	err := client.RemoveReplica(context.Background(), 1)
	if err != nil {
		t.Fatalf("RemoveReplica() error: %v", err)
	}
}

func TestAdminClient_Leave(t *testing.T) {
	addr := mockAdminServer(t, func(req adminRequest) AdminResponse {
		if req.Command != "leave" {
			return AdminResponse{OK: false, Message: "unexpected command"}
		}
		if req.Shard == nil || *req.Shard != 2 {
			return AdminResponse{OK: false, Message: "bad shard"}
		}
		return AdminResponse{OK: true, Message: "left shard 2"}
	})

	client := &AdminClient{Addr: addr, Timeout: 2 * time.Second}
	err := client.Leave(context.Background(), 2)
	if err != nil {
		t.Fatalf("Leave() error: %v", err)
	}
}

func TestAdminClient_ErrorResponse(t *testing.T) {
	addr := mockAdminServer(t, func(req adminRequest) AdminResponse {
		return AdminResponse{OK: false, Message: "shard 99 not found"}
	})

	client := &AdminClient{Addr: addr, Timeout: 2 * time.Second}
	_, err := client.Status(context.Background())
	if err == nil {
		t.Fatal("expected error for ok=false response")
	}
}

// mockTLSAdminServer starts a TLS-enabled admin mock server.
func mockTLSAdminServer(t *testing.T, serverTLS *tls.Config, handler func(req adminRequest) AdminResponse) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				scanner := bufio.NewScanner(conn)
				if !scanner.Scan() {
					return
				}
				var req adminRequest
				if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
					resp := AdminResponse{OK: false, Message: fmt.Sprintf("bad request: %v", err)}
					data, _ := json.Marshal(resp)
					_, _ = conn.Write(append(data, '\n'))
					return
				}
				resp := handler(req)
				data, _ := json.Marshal(resp)
				_, _ = conn.Write(append(data, '\n'))
			}()
		}
	}()
	return ln.Addr().String()
}

func TestAdminClient_TLS_Status(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := generateTestCerts(t, dir)

	// Build server TLS config requiring client certs (mTLS).
	serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatal(err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPEM)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}

	addr := mockTLSAdminServer(t, serverTLS, func(req adminRequest) AdminResponse {
		if req.Command != "status" {
			return AdminResponse{OK: false, Message: "unexpected command"}
		}
		return AdminResponse{
			OK:     true,
			Shards: []ShardInfo{{Shard: 0, ListenAddr: "10.0.0.1:6000"}},
		}
	})

	// Client with TLS.
	clientTLS, err := LoadTLSConfig(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}

	client := &AdminClient{Addr: addr, Timeout: 2 * time.Second, TLSConfig: clientTLS}
	resp, err := client.Status(context.Background())
	if err != nil {
		t.Fatalf("TLS Status() error: %v", err)
	}
	if !resp.OK {
		t.Fatalf("TLS Status() not ok: %s", resp.Message)
	}
	if len(resp.Shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(resp.Shards))
	}
}
