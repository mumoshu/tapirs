package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
	"github.com/mumoshu/tapirs/kubernetes/operator/internal/tapir"
)

const requeueDelay = 5 * time.Second

// podInfo holds the resolved address info for a pod in a StatefulSet.
type podInfo struct {
	Name  string // pod name (e.g. "mycluster-default-0")
	PodIP string // pod IP address
}

// reconcileBootstrap drives the cluster through its lifecycle phases.
// Returns true if the reconciler should requeue (still in progress),
// false if the cluster reached Running or no further action needed.
func (r *TAPIRClusterReconciler) reconcileBootstrap(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) (requeue bool, err error) {
	log := logf.FromContext(ctx)

	switch cluster.Status.Phase {
	case tapirv1alpha1.PhaseCreatingDiscovery:
		// Check if discovery StatefulSet is ready
		ready, err := r.isStatefulSetReady(ctx, cluster.Namespace, cluster.Name+"-discovery")
		if err != nil {
			return false, err
		}
		if !ready {
			log.Info("Waiting for discovery StatefulSet to be ready")
			return true, nil
		}
		return true, r.setPhase(ctx, cluster, tapirv1alpha1.PhaseBootstrappingDiscovery)

	case tapirv1alpha1.PhaseBootstrappingDiscovery:
		if err := r.bootstrapDiscovery(ctx, cluster); err != nil {
			log.Error(err, "Failed to bootstrap discovery")
			return true, nil // requeue to retry
		}
		return true, r.setPhase(ctx, cluster, tapirv1alpha1.PhaseCreatingDataPlane)

	case tapirv1alpha1.PhaseCreatingDataPlane:
		// Check shard-manager deployment ready
		ready, err := r.isDeploymentReady(ctx, cluster.Namespace, cluster.Name+"-shard-manager")
		if err != nil {
			return false, err
		}
		if !ready {
			log.Info("Waiting for shard-manager Deployment to be ready")
			return true, nil
		}
		// Check all node pool StatefulSets ready
		for _, pool := range cluster.Spec.NodePools {
			ready, err := r.isStatefulSetReady(ctx, cluster.Namespace, cluster.Name+"-"+pool.Name)
			if err != nil {
				return false, err
			}
			if !ready {
				log.Info("Waiting for node pool StatefulSet to be ready", "pool", pool.Name)
				return true, nil
			}
		}
		return true, r.setPhase(ctx, cluster, tapirv1alpha1.PhaseBootstrappingReplicas)

	case tapirv1alpha1.PhaseBootstrappingReplicas:
		if err := r.bootstrapReplicas(ctx, cluster); err != nil {
			log.Error(err, "Failed to bootstrap replicas")
			return true, nil // requeue to retry
		}
		return true, r.setPhase(ctx, cluster, tapirv1alpha1.PhaseRegisteringShards)

	case tapirv1alpha1.PhaseRegisteringShards:
		if err := r.registerShards(ctx, cluster); err != nil {
			log.Error(err, "Failed to register shards")
			return true, nil // requeue to retry
		}
		return true, r.setPhase(ctx, cluster, tapirv1alpha1.PhaseRunning)

	case tapirv1alpha1.PhaseRunning:
		if err := r.updateStatus(ctx, cluster); err != nil {
			return false, err
		}
		return false, nil

	default:
		return false, nil
	}
}

// bootstrapDiscovery bootstraps discovery replicas with static membership.
func (r *TAPIRClusterReconciler) bootstrapDiscovery(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	log := logf.FromContext(ctx)

	pods, err := r.getStatefulSetPodIPs(ctx, cluster.Namespace, cluster.Name+"-discovery", cluster.Spec.Discovery.Replicas)
	if err != nil {
		return fmt.Errorf("get discovery pod IPs: %w", err)
	}

	// Build membership list: all discovery pods at tapir port
	membership := make([]string, len(pods))
	for i, p := range pods {
		membership[i] = fmt.Sprintf("%s:%d", p.PodIP, discoveryTapirPort)
	}

	// Bootstrap each discovery pod with shard 0 (the discovery shard)
	for _, p := range pods {
		adminAddr := fmt.Sprintf("%s:%d", p.PodIP, adminPort)
		client := &tapir.AdminClient{Addr: adminAddr, Timeout: 10 * time.Second}

		// Check if already bootstrapped
		resp, err := client.Status(ctx)
		if err != nil {
			return fmt.Errorf("status %s: %w", adminAddr, err)
		}

		alreadyHasShard := false
		for _, s := range resp.Shards {
			if s.Shard == 0 {
				alreadyHasShard = true
				break
			}
		}
		if alreadyHasShard {
			log.Info("Discovery pod already has shard 0", "pod", p.Name)
			continue
		}

		listenAddr := fmt.Sprintf("%s:%d", p.PodIP, discoveryTapirPort)
		log.Info("Bootstrapping discovery replica", "pod", p.Name, "listenAddr", listenAddr)
		if err := client.AddReplica(ctx, 0, listenAddr, membership, "memory"); err != nil {
			return fmt.Errorf("add_replica on %s: %w", adminAddr, err)
		}
	}

	return nil
}

// bootstrapReplicas bootstraps data-node replicas with static membership per shard.
func (r *TAPIRClusterReconciler) bootstrapReplicas(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	log := logf.FromContext(ctx)

	// Collect all data-node pod IPs in order (by pool name, then pod index)
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

		// Placement: first shard.Replicas pods (round-robin)
		shardPods := allPods[:shard.Replicas]

		// Build membership for this shard
		membership := make([]string, len(shardPods))
		for i, p := range shardPods {
			membership[i] = fmt.Sprintf("%s:%d", p.PodIP, port)
		}

		// Bootstrap each replica
		for _, p := range shardPods {
			adminAddr := fmt.Sprintf("%s:%d", p.PodIP, adminPort)
			client := &tapir.AdminClient{Addr: adminAddr, Timeout: 10 * time.Second}

			// Check if already bootstrapped
			resp, err := client.Status(ctx)
			if err != nil {
				return fmt.Errorf("status %s: %w", adminAddr, err)
			}

			alreadyHasShard := false
			for _, s := range resp.Shards {
				if s.Shard == shard.Number {
					alreadyHasShard = true
					break
				}
			}
			if alreadyHasShard {
				log.Info("Pod already has shard", "pod", p.Name, "shard", shard.Number)
				continue
			}

			listenAddr := fmt.Sprintf("%s:%d", p.PodIP, port)
			log.Info("Bootstrapping data replica", "pod", p.Name, "shard", shard.Number, "listenAddr", listenAddr)
			if err := client.AddReplica(ctx, shard.Number, listenAddr, membership, storage); err != nil {
				return fmt.Errorf("add_replica shard %d on %s: %w", shard.Number, adminAddr, err)
			}
		}
	}

	return nil
}

// registerShards registers each shard's key range and replica addresses with the shard-manager.
func (r *TAPIRClusterReconciler) registerShards(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	log := logf.FromContext(ctx)

	allPods, err := r.getAllDataNodePods(ctx, cluster)
	if err != nil {
		return err
	}

	smClient := &tapir.ShardManagerClient{
		BaseURL: shardManagerURL(cluster),
	}

	for _, shard := range cluster.Spec.Shards {
		port := int32(replicaBasePort) + shard.Number
		shardPods := allPods[:shard.Replicas]

		replicas := make([]string, len(shardPods))
		for i, p := range shardPods {
			replicas[i] = fmt.Sprintf("%s:%d", p.PodIP, port)
		}

		log.Info("Registering shard", "shard", shard.Number,
			"keyRangeStart", shard.KeyRangeStart, "keyRangeEnd", shard.KeyRangeEnd,
			"replicas", replicas)

		if err := smClient.RegisterShard(ctx, shard.Number,
			shard.KeyRangeStart, shard.KeyRangeEnd, replicas); err != nil {
			return fmt.Errorf("register shard %d: %w", shard.Number, err)
		}
	}

	return nil
}

// updateStatus refreshes the cluster's status fields by querying admin endpoints.
func (r *TAPIRClusterReconciler) updateStatus(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	allPods, err := r.getAllDataNodePods(ctx, cluster)
	if err != nil {
		return err
	}

	// Update node status
	nodeStatuses := make([]tapirv1alpha1.NodeStatus, 0, len(allPods))
	for _, p := range allPods {
		adminAddr := fmt.Sprintf("%s:%d", p.PodIP, adminPort)
		client := &tapir.AdminClient{Addr: adminAddr, Timeout: 5 * time.Second}

		ns := tapirv1alpha1.NodeStatus{
			Name:      p.Name,
			PodIP:     p.PodIP,
			AdminAddr: adminAddr,
			Ready:     true,
		}

		resp, err := client.Status(ctx)
		if err != nil {
			ns.Ready = false
		} else {
			for _, s := range resp.Shards {
				ns.Shards = append(ns.Shards, tapirv1alpha1.NodeShardStatus{
					Number:     s.Shard,
					ListenAddr: s.ListenAddr,
				})
			}
		}
		nodeStatuses = append(nodeStatuses, ns)
	}
	cluster.Status.Nodes = nodeStatuses

	// Update shard status
	shardStatuses := make([]tapirv1alpha1.ShardStatus, 0, len(cluster.Spec.Shards))
	for _, shard := range cluster.Spec.Shards {
		port := int32(replicaBasePort) + shard.Number
		ss := tapirv1alpha1.ShardStatus{
			Number:     shard.Number,
			Registered: true, // we passed RegisteringShards phase
		}

		for _, p := range allPods[:shard.Replicas] {
			addr := fmt.Sprintf("%s:%d", p.PodIP, port)
			ss.Replicas = append(ss.Replicas, addr)
			// Count ready by checking if the pod has this shard in its node status
			for _, ns := range nodeStatuses {
				if ns.Name == p.Name && ns.Ready {
					for _, nss := range ns.Shards {
						if nss.Number == shard.Number {
							ss.ReadyReplicas++
							break
						}
					}
					break
				}
			}
		}
		shardStatuses = append(shardStatuses, ss)
	}
	cluster.Status.Shards = shardStatuses

	return r.Status().Update(ctx, cluster)
}

// getAllDataNodePods collects pod IPs from all node pool StatefulSets,
// ordered by pool name then pod index.
func (r *TAPIRClusterReconciler) getAllDataNodePods(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) ([]podInfo, error) {
	// Sort pools by name for deterministic ordering
	pools := make([]tapirv1alpha1.NodePool, len(cluster.Spec.NodePools))
	copy(pools, cluster.Spec.NodePools)
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name < pools[j].Name })

	var allPods []podInfo
	for _, pool := range pools {
		pods, err := r.getStatefulSetPodIPs(ctx, cluster.Namespace, cluster.Name+"-"+pool.Name, pool.Replicas)
		if err != nil {
			return nil, fmt.Errorf("get pod IPs for pool %s: %w", pool.Name, err)
		}
		allPods = append(allPods, pods...)
	}
	return allPods, nil
}

// getStatefulSetPodIPs returns the pod IPs for a StatefulSet by looking up
// individual pods by their deterministic names.
func (r *TAPIRClusterReconciler) getStatefulSetPodIPs(ctx context.Context, namespace, stsName string, replicas int32) ([]podInfo, error) {
	pods := make([]podInfo, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		podName := fmt.Sprintf("%s-%d", stsName, i)
		var pod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, &pod); err != nil {
			return nil, fmt.Errorf("get pod %s: %w", podName, err)
		}
		if pod.Status.PodIP == "" {
			return nil, fmt.Errorf("pod %s has no IP yet", podName)
		}
		pods = append(pods, podInfo{Name: podName, PodIP: pod.Status.PodIP})
	}
	return pods, nil
}

func (r *TAPIRClusterReconciler) isStatefulSetReady(ctx context.Context, namespace, name string) (bool, error) {
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &sts); err != nil {
		return false, err
	}
	if sts.Spec.Replicas == nil {
		return false, nil
	}
	return sts.Status.ReadyReplicas == *sts.Spec.Replicas, nil
}

func (r *TAPIRClusterReconciler) isDeploymentReady(ctx context.Context, namespace, name string) (bool, error) {
	var deploy appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &deploy); err != nil {
		return false, err
	}
	if deploy.Spec.Replicas == nil {
		return false, nil
	}
	return deploy.Status.ReadyReplicas == *deploy.Spec.Replicas, nil
}

func (r *TAPIRClusterReconciler) setPhase(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster, phase tapirv1alpha1.ClusterPhase) error {
	log := logf.FromContext(ctx)
	log.Info("Transitioning phase", "from", cluster.Status.Phase, "to", phase)
	cluster.Status.Phase = phase
	return r.Status().Update(ctx, cluster)
}
