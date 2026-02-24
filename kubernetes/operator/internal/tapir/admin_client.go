package tapir

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// AdminClient communicates with a tapirs node via the admin TCP JSON protocol.
// Each call opens a fresh TCP connection, sends one JSON line, reads the
// response, and closes — matching the server's one-command-per-connection model.
type AdminClient struct {
	// Addr is the admin TCP endpoint (e.g. "10.0.0.1:9000").
	Addr string

	// Timeout is the per-call dial+read timeout. Defaults to 5s.
	Timeout time.Duration

	// TLSConfig enables mTLS when non-nil. Use LoadTLSConfig() to create
	// a config with automatic certificate reloading.
	TLSConfig *tls.Config
}

type adminRequest struct {
	Command    string   `json:"command"`
	Shard      *int32   `json:"shard,omitempty"`
	ListenAddr string   `json:"listen_addr,omitempty"`
	Storage    string   `json:"storage,omitempty"`
	Membership []string `json:"membership,omitempty"`
}

// AdminResponse is the JSON response from the admin server.
type AdminResponse struct {
	OK      bool        `json:"ok"`
	Message string      `json:"message,omitempty"`
	Shards  []ShardInfo `json:"shards,omitempty"`
}

// ShardInfo describes a shard replica running on a node.
type ShardInfo struct {
	Shard      int32  `json:"shard"`
	ListenAddr string `json:"listen_addr"`
}

func (c *AdminClient) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 5 * time.Second
}

func (c *AdminClient) do(ctx context.Context, req adminRequest) (*AdminResponse, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(c.timeout())
	}

	d := net.Dialer{Timeout: c.timeout()}
	rawConn, err := d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return nil, fmt.Errorf("admin dial %s: %w", c.Addr, err)
	}
	defer func() { _ = rawConn.Close() }()

	conn := rawConn
	if c.TLSConfig != nil {
		host, _, _ := net.SplitHostPort(c.Addr)
		tlsConn := tls.Client(rawConn, &tls.Config{
			ServerName:           host,
			Certificates:         c.TLSConfig.Certificates,
			RootCAs:              c.TLSConfig.RootCAs,
			GetClientCertificate: c.TLSConfig.GetClientCertificate,
			MinVersion:           c.TLSConfig.MinVersion,
		})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return nil, fmt.Errorf("admin TLS handshake with %s: %w", c.Addr, err)
		}
		conn = tlsConn
	}

	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("admin set deadline: %w", err)
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("admin marshal request: %w", err)
	}
	data = append(data, '\n')

	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("admin write to %s: %w", c.Addr, err)
	}

	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("admin read from %s: %w", c.Addr, err)
		}
		return nil, fmt.Errorf("admin read from %s: connection closed without response", c.Addr)
	}

	var resp AdminResponse
	if err := json.Unmarshal(scanner.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("admin unmarshal response from %s: %w", c.Addr, err)
	}

	if !resp.OK {
		return &resp, fmt.Errorf("admin error from %s: %s", c.Addr, resp.Message)
	}
	return &resp, nil
}

// Status returns the list of shard replicas running on the node.
func (c *AdminClient) Status(ctx context.Context) (*AdminResponse, error) {
	return c.do(ctx, adminRequest{Command: "status"})
}

// AddReplica creates a shard replica on the node.
//
// If membership is non-empty, the replica is created with static membership
// (used during initial bootstrap). If membership is nil, the node will
// dynamically join via the shard-manager (used for runtime scaling).
func (c *AdminClient) AddReplica(ctx context.Context, shard int32, listenAddr string, membership []string, storage string) error {
	if storage == "" {
		storage = "memory"
	}
	_, err := c.do(ctx, adminRequest{
		Command:    "add_replica",
		Shard:      &shard,
		ListenAddr: listenAddr,
		Storage:    storage,
		Membership: membership,
	})
	return err
}

// RemoveReplica removes a shard replica from the node (local cleanup only).
func (c *AdminClient) RemoveReplica(ctx context.Context, shard int32) error {
	_, err := c.do(ctx, adminRequest{
		Command: "remove_replica",
		Shard:   &shard,
	})
	return err
}

// Leave gracefully removes the node from a shard's consensus group via the
// shard-manager. This should be called before RemoveReplica during scale-down.
func (c *AdminClient) Leave(ctx context.Context, shard int32) error {
	_, err := c.do(ctx, adminRequest{
		Command: "leave",
		Shard:   &shard,
	})
	return err
}
