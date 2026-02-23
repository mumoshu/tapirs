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
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(shardManagerPort)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
		},
	}

	var volumes []corev1.Volume
	injectTLS(&container, &volumes, cluster, "shard-manager")

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

// desiredNodePoolService returns the headless Service for a node pool.
func desiredNodePoolService(cluster *tapirv1alpha1.TAPIRCluster, pool tapirv1alpha1.NodePool) *corev1.Service {
	component := "node-" + pool.Name
	ports := []corev1.ServicePort{
		{Name: "admin", Port: adminPort},
	}
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

	containerPorts := []corev1.ContainerPort{
		{Name: "admin", ContainerPort: adminPort},
	}
	for _, shard := range cluster.Spec.Shards {
		port := int32(replicaBasePort) + shard.Number
		containerPorts = append(containerPorts, corev1.ContainerPort{
			Name:          fmt.Sprintf("tapir-%d", shard.Number),
			ContainerPort: port,
		})
	}

	container := corev1.Container{
		Name:            "tapir",
		Image:           cluster.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"tapi", "node"},
		Args: []string{
			fmt.Sprintf("--admin-listen-addr=0.0.0.0:%d", adminPort),
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
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(adminPort)},
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
