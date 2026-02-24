package tapir

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ShardManagerClient communicates with the tapirs shard-manager HTTP API.
type ShardManagerClient struct {
	// BaseURL is the shard-manager endpoint (e.g. "http://10.0.0.1:9001").
	BaseURL string

	// HTTPClient is the HTTP client to use. If nil, a default with 10s timeout is used.
	HTTPClient *http.Client
}

func (c *ShardManagerClient) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
}

type shardManagerResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func (c *ShardManagerClient) post(ctx context.Context, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("shard-manager marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("shard-manager create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("shard-manager POST %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("shard-manager read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var smResp shardManagerResponse
		if json.Unmarshal(respBody, &smResp) == nil && smResp.Error != "" {
			return fmt.Errorf("shard-manager POST %s (%d): %s", path, resp.StatusCode, smResp.Error)
		}
		return fmt.Errorf("shard-manager POST %s: HTTP %d: %s", path, resp.StatusCode, string(respBody))
	}

	return nil
}

type registerRequest struct {
	Shard         int32    `json:"shard"`
	KeyRangeStart *string  `json:"key_range_start"`
	KeyRangeEnd   *string  `json:"key_range_end"`
	Replicas      []string `json:"replicas,omitempty"`
}

// RegisterActiveShard makes an already configured and running shard's replicas
// visible to clients and other replicas in the cluster via the discovery cluster.
// Passing explicit replicas avoids the 10s CachingShardDirectory push race during
// initial bootstrap.
func (c *ShardManagerClient) RegisterActiveShard(ctx context.Context, shard int32, keyRangeStart, keyRangeEnd string, replicas []string) error {
	req := registerRequest{
		Shard:    shard,
		Replicas: replicas,
	}
	if keyRangeStart != "" {
		req.KeyRangeStart = &keyRangeStart
	}
	if keyRangeEnd != "" {
		req.KeyRangeEnd = &keyRangeEnd
	}
	return c.post(ctx, "/v1/register", req)
}

type splitRequest struct {
	Source      int32    `json:"source"`
	SplitKey    string   `json:"split_key"`
	NewShard    int32    `json:"new_shard"`
	NewReplicas []string `json:"new_replicas"`
}

// Split splits a shard at the given key. Out of scope for v1alpha1 operator
// reconciliation — exposed for future use or manual invocation.
func (c *ShardManagerClient) Split(ctx context.Context, source int32, splitKey string, newShard int32, newReplicas []string) error {
	return c.post(ctx, "/v1/split", splitRequest{
		Source:      source,
		SplitKey:    splitKey,
		NewShard:    newShard,
		NewReplicas: newReplicas,
	})
}

type mergeRequest struct {
	Absorbed  int32 `json:"absorbed"`
	Surviving int32 `json:"surviving"`
}

// Merge merges the absorbed shard into the surviving shard. Out of scope for
// v1alpha1 operator reconciliation.
func (c *ShardManagerClient) Merge(ctx context.Context, absorbed, surviving int32) error {
	return c.post(ctx, "/v1/merge", mergeRequest{
		Absorbed:  absorbed,
		Surviving: surviving,
	})
}
