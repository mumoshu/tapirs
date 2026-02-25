// pd-split: Pre-split TiKV regions at exact keys via PD gRPC API.
//
// Usage:
//
//	pd-split --pd-addr 127.0.0.1:2379 --split-keys "n,z"
//
// This calls PD's SplitAndScatterRegions gRPC API to split the default
// region at the specified keys. This is used to make TiKV's region count
// match TAPIR's shard count for fair benchmarking.
//
// Note: TiKV encodes keys internally (adding a 'z' prefix for data keys
// in the MVCC layer), so the actual region boundaries may not perfectly
// match the raw split keys. For benchmarking purposes this is close
// enough — the key space is evenly distributed regardless of encoding.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
)

func main() {
	pdAddr := flag.String("pd-addr", "127.0.0.1:2379", "PD address")
	splitKeysStr := flag.String("split-keys", "", "comma-separated split keys")
	flag.Parse()

	if *splitKeysStr == "" {
		fmt.Fprintln(os.Stderr, "no split keys provided, nothing to do")
		os.Exit(0)
	}

	keys := strings.Split(*splitKeysStr, ",")
	splitKeys := make([][]byte, len(keys))
	for i, k := range keys {
		splitKeys[i] = []byte(strings.TrimSpace(k))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := pd.NewClientWithContext(
		ctx,
		caller.Component("bench-pd-split"),
		[]string{*pdAddr},
		pd.SecurityOption{},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create PD client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	resp, err := client.SplitAndScatterRegions(ctx, splitKeys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "SplitAndScatterRegions failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("split complete: %d%% finished, %d regions\n",
		resp.GetSplitFinishedPercentage(), len(resp.GetRegionsId()))
}
