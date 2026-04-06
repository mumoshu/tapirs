package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPhase represents the lifecycle phase of a TAPIRCluster.
// +kubebuilder:validation:Enum=Pending;CreatingDiscovery;BootstrappingDiscovery;CreatingDataPlane;BootstrappingReplicas;RegisteringShards;Running;Updating;Failed
type ClusterPhase string

const (
	PhasePending                ClusterPhase = "Pending"
	PhaseCreatingDiscovery      ClusterPhase = "CreatingDiscovery"
	PhaseBootstrappingDiscovery ClusterPhase = "BootstrappingDiscovery"
	PhaseCreatingDataPlane      ClusterPhase = "CreatingDataPlane"
	PhaseBootstrappingReplicas  ClusterPhase = "BootstrappingReplicas"
	PhaseRegisteringShards      ClusterPhase = "RegisteringShards"
	PhaseRunning                ClusterPhase = "Running"
	PhaseUpdating               ClusterPhase = "Updating"
	PhaseFailed                 ClusterPhase = "Failed"
)

// TAPIRClusterSpec defines the desired state of a TAPIRCluster.
type TAPIRClusterSpec struct {
	// image is the container image for all tapirs components.
	// +required
	Image string `json:"image"`

	// discovery configures the TAPIR discovery store tier.
	// +required
	Discovery DiscoverySpec `json:"discovery"`

	// nodePools defines the data-node StatefulSets. Each pool maps to a
	// separate StatefulSet named {cluster}-{pool.name}.
	// +required
	// +kubebuilder:validation:MinItems=1
	NodePools []NodePool `json:"nodePools"`

	// shards defines the shard layout. Key ranges must cover the full
	// keyspace without gaps or overlaps.
	// +required
	// +kubebuilder:validation:MinItems=1
	Shards []ShardSpec `json:"shards"`

	// tls configures mutual TLS for all cluster communication. When enabled,
	// cert-manager Certificate resources are created for each component group,
	// and TLS credentials are mounted into all pods.
	// +optional
	TLS *TLSSpec `json:"tls,omitempty"`

	// destination configures where the cluster uploads segments and
	// manifests. When set, all replicas upload to S3 on every flush.
	// +optional
	Destination *DestinationSpec `json:"destination,omitempty"`

	// source configures this cluster to bootstrap from an existing S3 backup.
	// When set, data-node replicas are pre-populated with data from the source
	// instead of starting empty. The source S3 path must contain per-shard
	// manifests and segments uploaded by a running cluster's sync_to_remote.
	// +optional
	Source *SourceSpec `json:"source,omitempty"`
}

// SourceMode determines how the cluster uses the source data.
// +kubebuilder:validation:Enum=writableClone;readReplica
type SourceMode string

const (
	// SourceModeWritableClone creates a writable cluster pre-populated via COW.
	// After bootstrap, the cluster is fully independent of the source.
	SourceModeWritableClone SourceMode = "writableClone"
	// SourceModeReadReplica creates a read-only cluster that continuously
	// tracks the source's S3 state, auto-refreshing at the configured interval.
	SourceModeReadReplica SourceMode = "readReplica"
)

// SourceSpec configures bootstrap from an existing S3 backup.
type SourceSpec struct {
	// s3 specifies the S3 location containing the source cluster's data.
	// +required
	S3 S3Spec `json:"s3"`

	// mode determines how the source data is used.
	// "writableClone" creates an independent writable cluster via COW.
	// "readReplica" creates a read-only cluster that auto-refreshes.
	// +required
	Mode SourceMode `json:"mode"`

	// refreshInterval is how often to check S3 for new manifests in
	// readReplica mode. Example: "10s", "1m".
	// The reconciler rejects this field for writableClone mode.
	// +optional
	RefreshInterval string `json:"refreshInterval,omitempty"`

	// snapshotName references a CrossShardSnapshot JSON file on S3,
	// created by `tapictl snapshot create`. Required for writableClone mode.
	// The operator reads s3://{bucket}/{prefix}snapshots/{snapshotName}.json
	// to get per-shard clone parameters.
	// +optional
	SnapshotName string `json:"snapshotName,omitempty"`
}

// DestinationSpec configures where the cluster uploads segments and manifests.
type DestinationSpec struct {
	// s3 configures S3 as the upload target.
	// +required
	S3 S3Spec `json:"s3"`
}

// S3Spec configures S3 backend for remote segment and manifest storage.
type S3Spec struct {
	// bucket is the S3 bucket name.
	// +required
	Bucket string `json:"bucket"`

	// prefix is the cluster-level key prefix (e.g. "prod/").
	// Each replica derives its shard prefix internally.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// endpoint is a custom S3-compatible endpoint URL (e.g. MinIO).
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// region is the AWS region.
	// +optional
	Region string `json:"region,omitempty"`

	// credentialsSecret is the name of a Kubernetes Secret containing
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. When set, envFrom is
	// added to node containers. For production use, prefer IAM roles
	// (IRSA/pod identity) and leave this empty.
	// +optional
	CredentialsSecret string `json:"credentialsSecret,omitempty"`
}

// TLSSpec configures mutual TLS for all cluster communication.
type TLSSpec struct {
	// enabled activates mTLS for all inter-component connections.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// issuerRef references a cert-manager Issuer or ClusterIssuer that signs
	// TLS certificates for cluster components.
	// +optional
	IssuerRef *IssuerRef `json:"issuerRef,omitempty"`
}

// IssuerRef references a cert-manager issuer.
type IssuerRef struct {
	// name is the name of the issuer.
	// +required
	Name string `json:"name"`

	// kind is the kind of the issuer (Issuer or ClusterIssuer).
	// +kubebuilder:default=ClusterIssuer
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	Kind string `json:"kind,omitempty"`
}

// DiscoverySpec configures the discovery store tier (a self-contained TAPIR
// cluster that stores shard topology metadata).
type DiscoverySpec struct {
	// replicas is the number of discovery pods.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas"`

	// resources defines CPU/memory requests and limits for discovery pods.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// NodePool defines a group of data-node pods backed by a StatefulSet.
type NodePool struct {
	// name is a unique identifier for this pool. The resulting StatefulSet
	// is named {cluster}-{name}.
	// +required
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`
	Name string `json:"name"`

	// replicas is the number of pods in this pool.
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// resources defines CPU/memory requests and limits for pods in this pool.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ShardSpec defines a single shard's configuration.
type ShardSpec struct {
	// number is the shard identifier.
	// +required
	// +kubebuilder:validation:Minimum=0
	Number int32 `json:"number"`

	// replicas is the number of TAPIR replicas for this shard. Must be <=
	// total pods across all node pools.
	// +required
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas"`

	// keyRangeStart is the inclusive lower bound of this shard's key range.
	// Empty means the minimum key.
	// +optional
	KeyRangeStart string `json:"keyRangeStart,omitempty"`

	// keyRangeEnd is the exclusive upper bound of this shard's key range.
	// Empty means the maximum key.
	// +optional
	KeyRangeEnd string `json:"keyRangeEnd,omitempty"`

	// storage is the backend type: "memory" (default) or "disk".
	// +kubebuilder:default=memory
	// +kubebuilder:validation:Enum=memory;disk
	// +optional
	Storage string `json:"storage,omitempty"`
}

// TAPIRClusterStatus defines the observed state of a TAPIRCluster.
type TAPIRClusterStatus struct {
	// phase is the high-level lifecycle phase of the cluster.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// discoveryEndpoint is the endpoint clients use to connect via
	// --discovery-tapir-endpoint (e.g. srv://{name}-discovery.{ns}.svc.cluster.local:6000).
	// +optional
	DiscoveryEndpoint string `json:"discoveryEndpoint,omitempty"`

	// shardManagerURL is the HTTP URL for the shard-manager service.
	// +optional
	ShardManagerURL string `json:"shardManagerURL,omitempty"`

	// nodes lists the observed state of each data-node pod.
	// +optional
	Nodes []NodeStatus `json:"nodes,omitempty"`

	// shards lists the observed state of each shard.
	// +optional
	Shards []ShardStatus `json:"shards,omitempty"`

	// conditions represent the current state of the TAPIRCluster.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// NodeStatus is the observed state of a single data-node pod.
type NodeStatus struct {
	// name is the pod name (e.g. "mycluster-default-0").
	Name string `json:"name"`

	// podIP is the pod's IP address.
	// +optional
	PodIP string `json:"podIP,omitempty"`

	// ready indicates whether the pod is ready.
	Ready bool `json:"ready"`

	// adminAddr is the admin TCP endpoint (podIP:9000).
	// +optional
	AdminAddr string `json:"adminAddr,omitempty"`

	// shards lists the TAPIR shard replicas hosted on this node.
	// +optional
	Shards []NodeShardStatus `json:"shards,omitempty"`
}

// NodeShardStatus describes a shard replica hosted on a node.
type NodeShardStatus struct {
	// number is the shard identifier.
	Number int32 `json:"number"`

	// listenAddr is the TAPIR protocol address (podIP:6000+shard).
	// +optional
	ListenAddr string `json:"listenAddr,omitempty"`

	// storage is the backend type ("memory" or "disk").
	// +optional
	Storage string `json:"storage,omitempty"`
}

// ShardStatus is the observed state of a single shard.
type ShardStatus struct {
	// number is the shard identifier.
	Number int32 `json:"number"`

	// readyReplicas is the number of healthy replicas for this shard.
	ReadyReplicas int32 `json:"readyReplicas"`

	// replicas lists the current membership addresses.
	// +optional
	Replicas []string `json:"replicas,omitempty"`

	// registered indicates whether the shard's key range has been registered
	// with the shard-manager.
	Registered bool `json:"registered"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Discovery",type=string,JSONPath=`.status.discoveryEndpoint`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TAPIRCluster is the Schema for the tapirclusters API.
type TAPIRCluster struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec TAPIRClusterSpec `json:"spec"`

	// +optional
	Status TAPIRClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TAPIRClusterList contains a list of TAPIRCluster.
type TAPIRClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TAPIRCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TAPIRCluster{}, &TAPIRClusterList{})
}
