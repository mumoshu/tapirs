package controller

import (
	"context"
	"fmt"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
)

// reconcileScaleUp checks whether any shards need more replicas and adds them
// using the dynamic shard-manager join path (no static membership).
// This is safe after initial bootstrap because discovery already has membership.
//
// Node pool StatefulSet replicas are already reconciled in reconcileNodePools
// (resource creation phase), so the only scale-up work here is adding TAPIR
// replicas to pods that don't yet host them.
func (r *TAPIRClusterReconciler) reconcileScaleUp(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	log := logf.FromContext(ctx)

	allPods, err := r.getAllDataNodePods(ctx, cluster)
	if err != nil {
		return err
	}

	for _, shard := range cluster.Spec.Shards {
		if int(shard.Replicas) > len(allPods) {
			return fmt.Errorf("shard %d requires %d replicas but only %d pods available",
				shard.Number, shard.Replicas, len(allPods))
		}

		port := int32(replicaBasePort) + shard.Number
		storage := shard.Storage
		if storage == "" {
			storage = "memory"
		}

		// Target pods for this shard (round-robin placement)
		shardPods := allPods[:shard.Replicas]

		for _, p := range shardPods {
			client := r.adminClient(ctx, cluster, p, p.ServiceName)

			resp, err := client.Status(ctx)
			if err != nil {
				return fmt.Errorf("status %s: %w", client.Addr, err)
			}

			alreadyHasShard := false
			for _, s := range resp.Shards {
				if s.Shard == shard.Number {
					alreadyHasShard = true
					break
				}
			}
			if alreadyHasShard {
				continue
			}

			// Dynamic join: add_replica without membership
			// The node will call POST /v1/join on the shard-manager
			listenAddr := fmt.Sprintf("%s:%d", p.PodIP, port)
			log.Info("Scaling up: adding replica via dynamic join",
				"pod", p.Name, "shard", shard.Number, "listenAddr", listenAddr)
			if err := client.AddReplica(ctx, shard.Number, listenAddr, nil, storage); err != nil {
				return fmt.Errorf("add_replica shard %d on %s: %w", shard.Number, client.Addr, err)
			}

			// Wait for view change to complete before adding next replica
			time.Sleep(3 * time.Second)
		}
	}

	return nil
}

// reconcileScaleDown checks whether any shards have excess replicas and removes
// them gracefully (leave via shard-manager, then local cleanup).
// TAPIR replicas must be removed BEFORE scaling down the StatefulSet pods.
func (r *TAPIRClusterReconciler) reconcileScaleDown(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	log := logf.FromContext(ctx)

	allPods, err := r.getAllDataNodePods(ctx, cluster)
	if err != nil {
		return err
	}

	for _, shard := range cluster.Spec.Shards {
		port := int32(replicaBasePort) + shard.Number

		// Find which pods currently host this shard but shouldn't
		// Target pods = first shard.Replicas pods (round-robin)
		targetPodNames := make(map[string]bool)
		upperBound := int(shard.Replicas)
		if upperBound > len(allPods) {
			upperBound = len(allPods)
		}
		for _, p := range allPods[:upperBound] {
			targetPodNames[p.Name] = true
		}

		// Check all pods for excess replicas of this shard
		for _, p := range allPods {
			if targetPodNames[p.Name] {
				continue // this pod should keep the shard
			}

			client := r.adminClient(ctx, cluster, p, p.ServiceName)

			resp, err := client.Status(ctx)
			if err != nil {
				log.Error(err, "Failed to get status during scale-down", "pod", p.Name)
				continue
			}

			hasShard := false
			for _, s := range resp.Shards {
				if s.Shard == shard.Number {
					hasShard = true
					break
				}
			}
			if !hasShard {
				continue
			}

			// Gracefully remove: leave first, then remove_replica
			log.Info("Scaling down: leaving shard",
				"pod", p.Name, "shard", shard.Number,
				"listenAddr", fmt.Sprintf("%s:%d", p.PodIP, port))

			if err := client.Leave(ctx, shard.Number); err != nil {
				return fmt.Errorf("leave shard %d on %s: %w", shard.Number, client.Addr, err)
			}

			// Wait for RemoveMember view change to propagate
			time.Sleep(3 * time.Second)

			if err := client.RemoveReplica(ctx, shard.Number); err != nil {
				return fmt.Errorf("remove_replica shard %d on %s: %w", shard.Number, client.Addr, err)
			}
		}
	}

	return nil
}
