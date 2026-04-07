package controller

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
)

const (
	discoveryTapirPort = 6000
	adminPort          = 9000
	metricsPort        = 9090
	shardManagerPort   = 9001
	replicaBasePort    = 6000
)

func labels(cluster *tapirv1alpha1.TAPIRCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "tapirs",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/component":  component,
		"app.kubernetes.io/managed-by": "tapirs-operator",
	}
}

func discoveryEndpoint(cluster *tapirv1alpha1.TAPIRCluster) string {
	return fmt.Sprintf("srv://%s-discovery.%s.svc.cluster.local:%d",
		cluster.Name, cluster.Namespace, discoveryTapirPort)
}

func shardManagerURL(cluster *tapirv1alpha1.TAPIRCluster) string {
	return shardManagerURLForCluster(cluster)
}

// desiredDiscoveryService returns the headless Service for discovery pods.
func desiredDiscoveryService(cluster *tapirv1alpha1.TAPIRCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-discovery",
			Namespace: cluster.Namespace,
			Labels:    labels(cluster, "discovery"),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Selector:                 labels(cluster, "discovery"),
			Ports: []corev1.ServicePort{
				{Name: "tapir", Port: discoveryTapirPort},
				{Name: "admin", Port: adminPort},
			},
		},
	}
}

// desiredDiscoveryStatefulSet returns the StatefulSet for discovery pods.
func desiredDiscoveryStatefulSet(cluster *tapirv1alpha1.TAPIRCluster) *appsv1.StatefulSet {
	replicas := cluster.Spec.Discovery.Replicas

	container := corev1.Container{
		Name:            "tapir",
		Image:           cluster.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"tapi", "node"},
		Args: []string{
			fmt.Sprintf("--admin-listen-addr=0.0.0.0:%d", adminPort),
			"--persist-dir=/data",
		},
		Env: []corev1.EnvVar{
			{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
			}},
			{Name: "RUST_LOG", Value: "info"},
		},
		Ports: []corev1.ContainerPort{
			{Name: "tapir", ContainerPort: discoveryTapirPort},
			{Name: "admin", ContainerPort: adminPort},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(adminPort)},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       3,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/data"},
		},
	}
	if cluster.Spec.Discovery.Resources != nil {
		container.Resources = *cluster.Spec.Discovery.Resources
	}

	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	injectTLS(&container, &volumes, cluster, "discovery")
	// Discovery does NOT get S3 args. Discovery is in-memory only and must
	// not upload to the same S3 bucket as data nodes — their shard_0/
	// prefixes would collide, causing clones to read discovery manifests
	// instead of data manifests.

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-discovery",
			Namespace: cluster.Namespace,
			Labels:    labels(cluster, "discovery"),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: cluster.Name + "-discovery",
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels(cluster, "discovery")},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(cluster, "discovery")},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
					Volumes:    volumes,
				},
			},
		},
	}
}

// desiredShardManagerService returns the ClusterIP Service for the shard-manager.
func desiredShardManagerService(cluster *tapirv1alpha1.TAPIRCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-shard-manager",
			Namespace: cluster.Namespace,
			Labels:    labels(cluster, "shard-manager"),
		},
		Spec: corev1.ServiceSpec{
			Selector: labels(cluster, "shard-manager"),
			Ports: []corev1.ServicePort{
				{Name: "http", Port: shardManagerPort},
			},
		},
	}
}

// desiredShardManagerDeployment returns the Deployment for the shard-manager.
func desiredShardManagerDeployment(cluster *tapirv1alpha1.TAPIRCluster) *appsv1.Deployment {
	var replicas int32 = 1

	container := corev1.Container{
		Name:            "shard-manager",
		Image:           cluster.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"tapi", "shard-manager"},
		Args: []string{
			fmt.Sprintf("--listen-addr=0.0.0.0:%d", shardManagerPort),
			"--discovery-tapir-endpoint=" + discoveryEndpoint(cluster),
		},
		Env: []corev1.EnvVar{
			{Name: "RUST_LOG", Value: "info,tapirs::discovery=debug"},
		},
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: shardManagerPort},
		},
	}

	if tlsEnabled(cluster) {
		// K8s HTTP probes cannot do mTLS — use exec probe with CLI healthcheck.
		container.ReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: shardManagerHealthcheckCommand(cluster),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      10,
		}
	} else {
		container.ReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intstr.FromInt32(shardManagerPort),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      10,
		}
	}

	var volumes []corev1.Volume
	injectTLS(&container, &volumes, cluster, "shard-manager")
	// Note: shard-manager uses `tapi shard-manager` which does not accept S3 flags.
	// Only `tapi node` (discovery and data node containers) supports --s3-bucket.

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-shard-manager",
			Namespace: cluster.Namespace,
			Labels:    labels(cluster, "shard-manager"),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels(cluster, "shard-manager")},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(cluster, "shard-manager")},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
					Volumes:    volumes,
				},
			},
		},
	}
}

// shardManagerHealthcheckCommand builds the exec probe command for shard-manager
// readiness checks when TLS is enabled. Runs `tapi shard-manager-healthz` with
// localhost URL and TLS flags. The probe runs inside the shard-manager pod.
func shardManagerHealthcheckCommand(cluster *tapirv1alpha1.TAPIRCluster) []string {
	scheme := "http"
	if tlsEnabled(cluster) {
		scheme = "https"
	}
	url := fmt.Sprintf("%s://127.0.0.1:%d", scheme, shardManagerPort)
	cmd := []string{
		"tapi", "shard-manager-healthz",
		"--shard-manager-url=" + url,
	}
	if tlsEnabled(cluster) {
		cmd = append(cmd, tlsArgs(cluster)...)
	}
	return cmd
}

// desiredNodePoolService returns the headless Service for a node pool.
func desiredNodePoolService(cluster *tapirv1alpha1.TAPIRCluster, pool tapirv1alpha1.NodePool) *corev1.Service {
	component := "node-" + pool.Name
	ports := make([]corev1.ServicePort, 0, 2+len(cluster.Spec.Shards))
	ports = append(ports, corev1.ServicePort{Name: "admin", Port: adminPort})
	ports = append(ports, corev1.ServicePort{Name: "metrics", Port: metricsPort})
	for _, shard := range cluster.Spec.Shards {
		port := int32(replicaBasePort) + shard.Number
		ports = append(ports, corev1.ServicePort{
			Name: fmt.Sprintf("tapir-%d", shard.Number),
			Port: port,
		})
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-" + pool.Name,
			Namespace: cluster.Namespace,
			Labels:    labels(cluster, component),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels(cluster, component),
			Ports:     ports,
		},
	}
}

// desiredNodePoolStatefulSet returns the StatefulSet for a node pool.
func desiredNodePoolStatefulSet(cluster *tapirv1alpha1.TAPIRCluster, pool tapirv1alpha1.NodePool) *appsv1.StatefulSet {
	component := "node-" + pool.Name

	containerPorts := make([]corev1.ContainerPort, 0, 1+len(cluster.Spec.Shards))
	containerPorts = append(containerPorts, corev1.ContainerPort{Name: "admin", ContainerPort: adminPort})
	for _, shard := range cluster.Spec.Shards {
		port := int32(replicaBasePort) + shard.Number
		containerPorts = append(containerPorts, corev1.ContainerPort{
			Name:          fmt.Sprintf("tapir-%d", shard.Number),
			ContainerPort: port,
		})
	}

	containerPorts = append(containerPorts, corev1.ContainerPort{
		Name: "metrics", ContainerPort: metricsPort,
	})

	container := corev1.Container{
		Name:            "tapir",
		Image:           cluster.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"tapi", "node"},
		Args: []string{
			fmt.Sprintf("--admin-listen-addr=0.0.0.0:%d", adminPort),
			fmt.Sprintf("--metrics-listen-addr=0.0.0.0:%d", metricsPort),
			"--persist-dir=/data",
			"--discovery-tapir-endpoint=" + discoveryEndpoint(cluster),
			"--shard-manager-url=" + shardManagerURL(cluster),
		},
		Env: []corev1.EnvVar{
			{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
			}},
			{Name: "RUST_LOG", Value: "info"},
		},
		Ports: containerPorts,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/readyz",
					Port: intstr.FromInt32(metricsPort),
				},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       3,
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/data"},
		},
	}
	if pool.Resources != nil {
		container.Resources = *pool.Resources
	}

	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	injectTLS(&container, &volumes, cluster, pool.Name)
	injectS3(&container, cluster)
	// Inject source S3 credentials so data nodes can download segments
	// from the source bucket during clone/read-replica bootstrap.
	if cluster.Spec.Source != nil && cluster.Spec.Source.S3.CredentialsSecret != "" {
		container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: cluster.Spec.Source.S3.CredentialsSecret,
				},
			},
		})
	}

	replicas := pool.Replicas
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-" + pool.Name,
			Namespace: cluster.Namespace,
			Labels:    labels(cluster, component),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: cluster.Name + "-" + pool.Name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels(cluster, component)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(cluster, component)},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
					Volumes:    volumes,
				},
			},
		},
	}
}

// injectS3 adds S3 CLI args and optional credentials to a container when destination S3 is configured.
func injectS3(container *corev1.Container, cluster *tapirv1alpha1.TAPIRCluster) {
	if cluster.Spec.Destination == nil {
		return
	}
	s3 := &cluster.Spec.Destination.S3
	container.Args = append(container.Args, fmt.Sprintf("--s3-bucket=%s", s3.Bucket))
	if s3.Prefix != "" {
		container.Args = append(container.Args, fmt.Sprintf("--s3-prefix=%s", s3.Prefix))
	}
	if s3.Endpoint != "" {
		container.Args = append(container.Args, fmt.Sprintf("--s3-endpoint=%s", s3.Endpoint))
	}
	if s3.Region != "" {
		container.Args = append(container.Args, fmt.Sprintf("--s3-region=%s", s3.Region))
	}
	if s3.CredentialsSecret != "" {
		container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: s3.CredentialsSecret},
			},
		})
	}
}
