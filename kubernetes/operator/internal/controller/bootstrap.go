package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	tapir "github.com/mumoshu/tapirs/kubernetes/operator/internal/tapir"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
)

const requeueDelay = 5 * time.Second

// podInfo holds the resolved address info for a pod in a StatefulSet.
type podInfo struct {
	Name        string // pod name (e.g. "mycluster-default-0")
	PodIP       string // pod IP address
	ServiceName string // headless service name (e.g. "mycluster-default")
}

// reconcileBootstrap drives the cluster through its lifecycle phases.
// Returns true if the reconciler should requeue (still in progress),
// false if the cluster reached Running or no further action needed.
func (r *TAPIRClusterReconciler) reconcileBootstrap(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) (requeue bool, err error) {
	log := logf.FromContext(ctx)

	// Validate source spec early.
	if src := cluster.Spec.Source; src != nil {
		if src.Mode == tapirv1alpha1.SourceModeWritableClone && src.RefreshInterval != "" {
			return false, fmt.Errorf("refreshInterval is only valid for readReplica mode, not writableClone")
		}
		// Reject source == destination: cloning from your own bucket
		// would read back your own discovery data or cause cycles.
		if dst := cluster.Spec.Destination; dst != nil {
			if src.S3.Bucket == dst.S3.Bucket && src.S3.Prefix == dst.S3.Prefix {
				return false, fmt.Errorf("source and destination S3 bucket/prefix must differ; "+
					"source bucket=%q prefix=%q matches destination", src.S3.Bucket, src.S3.Prefix)
			}
		}
	}

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
		// Check all node pool StatefulSets have all pods running (not
		// necessarily Ready — pods need shard registration + view change
		// before the /readyz probe passes). Full readiness is checked
		// later in WaitingForQuorum.
		for _, pool := range cluster.Spec.NodePools {
			running, err := r.isStatefulSetRunning(ctx, cluster.Namespace, cluster.Name+"-"+pool.Name)
			if err != nil {
				return false, err
			}
			if !running {
				log.Info("Waiting for node pool StatefulSet pods to be running", "pool", pool.Name)
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
		return true, r.setPhase(ctx, cluster, tapirv1alpha1.PhaseWaitingForQuorum)

	case tapirv1alpha1.PhaseWaitingForQuorum:
		// Gate Running on quorum: each shard needs readyReplicas >= f+1.
		// The data-node readiness probe is now HTTP /readyz which returns
		// 200 only after all local shards complete their first view change,
		// so readyReplicas reflects actual IR quorum participation.
		for _, pool := range cluster.Spec.NodePools {
			ready, err := r.getStatefulSetReadyReplicas(ctx, cluster.Namespace, cluster.Name+"-"+pool.Name)
			if err != nil {
				return false, err
			}
			for _, shard := range cluster.Spec.Shards {
				f := (shard.Replicas - 1) / 3
				quorum := f + 1
				if ready < quorum {
					log.Info("Waiting for quorum", "pool", pool.Name,
						"readyReplicas", ready, "needed", quorum)
					return true, nil
				}
			}
		}
		return true, r.setPhase(ctx, cluster, tapirv1alpha1.PhaseRunning)

	case tapirv1alpha1.PhaseRunning:
		// Handle scale-down first (remove replicas before pods)
		if err := r.reconcileScaleDown(ctx, cluster); err != nil {
			log.Error(err, "Failed to reconcile scale-down")
			return true, nil
		}
		// Handle scale-up (add replicas to new/empty pods)
		if err := r.reconcileScaleUp(ctx, cluster); err != nil {
			log.Error(err, "Failed to reconcile scale-up")
			return true, nil
		}
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
	discoverySvc := cluster.Name + "-discovery"
	for _, p := range pods {
		client := r.adminClient(ctx, cluster, p, discoverySvc)

		// Check if already bootstrapped
		resp, err := client.Status(ctx)
		if err != nil {
			return fmt.Errorf("status %s: %w", client.Addr, err)
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
		if err := client.AddReplica(ctx, 0, listenAddr, membership, tapir.DefaultStorage, "discovery"); err != nil {
			return fmt.Errorf("add_replica on %s: %w", client.Addr, err)
		}
	}

	return nil
}

// crossShardSnapshot matches the Rust CrossShardSnapshot JSON format.
type crossShardSnapshot struct {
	Timestamp string                       `json:"timestamp"`
	CutoffTs  uint64                       `json:"cutoff_ts"`
	CeilingTs uint64                       `json:"ceiling_ts"`
	Shards    map[string]shardSnapshotInfo `json:"shards"`
}

type shardSnapshotInfo struct {
	ManifestView uint64 `json:"manifest_view"`
}

// readSnapshotFromS3 downloads and parses a CrossShardSnapshot JSON file from S3.
func (r *TAPIRClusterReconciler) readSnapshotFromS3(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) (*crossShardSnapshot, error) {
	src := cluster.Spec.Source
	key := src.S3.Prefix + "snapshots/" + src.SnapshotName + ".json"

	// Read S3 credentials from the referenced K8s Secret.
	var opts []func(*awsconfig.LoadOptions) error
	if src.S3.CredentialsSecret != "" {
		var secret corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{
			Name:      src.S3.CredentialsSecret,
			Namespace: cluster.Namespace,
		}, &secret); err != nil {
			return nil, fmt.Errorf("get credentials secret %q: %w", src.S3.CredentialsSecret, err)
		}
		accessKey := string(secret.Data["AWS_ACCESS_KEY_ID"])
		secretKey := string(secret.Data["AWS_SECRET_ACCESS_KEY"])
		opts = append(opts, awsconfig.WithCredentialsProvider(
			awscreds.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	if src.S3.Region != "" {
		opts = append(opts, awsconfig.WithRegion(src.S3.Region))
	} else {
		opts = append(opts, awsconfig.WithRegion("us-east-1"))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if src.S3.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(src.S3.Endpoint)
			o.UsePathStyle = true
		})
	}
	client := s3.NewFromConfig(cfg, s3Opts...)

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(src.S3.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get snapshot s3://%s/%s: %w", src.S3.Bucket, key, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read snapshot body: %w", err)
	}

	var snapshot crossShardSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return nil, fmt.Errorf("parse snapshot JSON: %w", err)
	}
	return &snapshot, nil
}

// bootstrapReplicas bootstraps data-node replicas with static membership per shard.
func (r *TAPIRClusterReconciler) bootstrapReplicas(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	log := logf.FromContext(ctx)

	// Collect all data-node pod IPs in order (by pool name, then pod index)
	allPods, err := r.getAllDataNodePods(ctx, cluster)
	if err != nil {
		return err
	}

	// For writable clones, read the snapshot from S3 before the shard loop.
	var snapshot *crossShardSnapshot
	if src := cluster.Spec.Source; src != nil && src.Mode == tapirv1alpha1.SourceModeWritableClone {
		if src.SnapshotName == "" {
			return fmt.Errorf("snapshotName is required for writableClone mode")
		}
		snapshot, err = r.readSnapshotFromS3(ctx, cluster)
		if err != nil {
			return fmt.Errorf("read snapshot: %w", err)
		}
		log.Info("Read snapshot from S3", "snapshot", src.SnapshotName,
			"cutoffTs", snapshot.CutoffTs, "ceilingTs", snapshot.CeilingTs,
			"shards", len(snapshot.Shards))
	}

	for _, shard := range cluster.Spec.Shards {
		if int(shard.Replicas) > len(allPods) {
			return fmt.Errorf("shard %d requires %d replicas but only %d pods available",
				shard.Number, shard.Replicas, len(allPods))
		}

		port := int32(replicaBasePort) + shard.Number
		storage := shard.Storage
		if storage == "" {
			storage = tapir.DefaultStorage
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
			client := r.adminClient(ctx, cluster, p, p.ServiceName)

			// Check if already bootstrapped
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
				log.Info("Pod already has shard", "pod", p.Name, "shard", shard.Number)
				continue
			}

			listenAddr := fmt.Sprintf("%s:%d", p.PodIP, port)
			log.Info("Bootstrapping data replica", "pod", p.Name, "shard", shard.Number, "listenAddr", listenAddr)

			if cluster.Spec.Source != nil {
				src := cluster.Spec.Source
				s3Source := tapir.S3SourceConfig{
					Bucket:   src.S3.Bucket,
					Prefix:   src.S3.Prefix,
					Endpoint: src.S3.Endpoint,
					Region:   src.S3.Region,
				}
				switch src.Mode {
				case tapirv1alpha1.SourceModeWritableClone:
					shardKey := fmt.Sprintf("%d", shard.Number)
					shardInfo, ok := snapshot.Shards[shardKey]
					if !ok {
						return fmt.Errorf("snapshot has no entry for shard %d", shard.Number)
					}
					snapshotParams := tapir.SnapshotParams{
						CutoffTs:     snapshot.CutoffTs,
						CeilingTs:    snapshot.CeilingTs,
						ManifestView: shardInfo.ManifestView,
					}
					if err := client.AddWritableCloneFromS3(ctx, shard.Number, listenAddr, membership, storage, s3Source, snapshotParams); err != nil {
						return fmt.Errorf("add_writable_clone_from_s3 shard %d on %s: %w", shard.Number, client.Addr, err)
					}
				case tapirv1alpha1.SourceModeReadReplica:
					refreshSecs := int64(30)
					if src.RefreshInterval != "" {
						d, parseErr := time.ParseDuration(src.RefreshInterval)
						if parseErr != nil {
							return fmt.Errorf("invalid refreshInterval %q: %w", src.RefreshInterval, parseErr)
						}
						refreshSecs = int64(d.Seconds())
					}
					if err := client.AddReadReplicaFromS3(ctx, shard.Number, listenAddr, s3Source, refreshSecs); err != nil {
						return fmt.Errorf("add_read_replica_from_s3 shard %d on %s: %w", shard.Number, client.Addr, err)
					}
				}
			} else {
				if err := client.AddReplica(ctx, shard.Number, listenAddr, membership, storage, "data"); err != nil {
					return fmt.Errorf("add_replica shard %d on %s: %w", shard.Number, client.Addr, err)
				}
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

	smClient := r.shardManagerClient(ctx, cluster)

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

		start := time.Now()
		if err := smClient.RegisterActiveShard(ctx, shard.Number,
			shard.KeyRangeStart, shard.KeyRangeEnd, replicas); err != nil {
			log.Error(err, "Shard registration failed", "shard", shard.Number, "elapsed", time.Since(start))
			return fmt.Errorf("register shard %d: %w", shard.Number, err)
		}
		log.Info("Shard registered successfully", "shard", shard.Number, "elapsed", time.Since(start))
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
		client := r.adminClient(ctx, cluster, p, p.ServiceName)

		ns := tapirv1alpha1.NodeStatus{
			Name:      p.Name,
			PodIP:     p.PodIP,
			AdminAddr: client.Addr,
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
	for i := range replicas {
		podName := fmt.Sprintf("%s-%d", stsName, i)
		var pod corev1.Pod
		if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, &pod); err != nil {
			return nil, fmt.Errorf("get pod %s: %w", podName, err)
		}
		if pod.Status.PodIP == "" {
			return nil, fmt.Errorf("pod %s has no IP yet", podName)
		}
		pods = append(pods, podInfo{Name: podName, PodIP: pod.Status.PodIP, ServiceName: stsName})
	}
	return pods, nil
}

func (r *TAPIRClusterReconciler) getStatefulSetReadyReplicas(ctx context.Context, namespace, name string) (int32, error) {
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &sts); err != nil {
		return 0, err
	}
	return sts.Status.ReadyReplicas, nil
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

// isStatefulSetRunning checks that all pods in the StatefulSet are created
// and running (but not necessarily Ready). Used during CreatingDataPlane
// where pods need shard registration before the /readyz probe passes.
func (r *TAPIRClusterReconciler) isStatefulSetRunning(ctx context.Context, namespace, name string) (bool, error) {
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &sts); err != nil {
		return false, err
	}
	if sts.Spec.Replicas == nil {
		return false, nil
	}
	return sts.Status.Replicas == *sts.Spec.Replicas, nil
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
