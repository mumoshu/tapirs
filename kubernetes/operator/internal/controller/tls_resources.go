package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
	"github.com/mumoshu/tapirs/kubernetes/operator/internal/tapir"
)

const (
	tlsMountPath         = "/tls"
	certManagerAPIVersion = "cert-manager.io/v1"
)

// tlsEnabled returns true if the cluster has TLS enabled with a valid issuerRef.
func tlsEnabled(cluster *tapirv1alpha1.TAPIRCluster) bool {
	return cluster.Spec.TLS != nil &&
		cluster.Spec.TLS.Enabled &&
		cluster.Spec.TLS.IssuerRef != nil
}

// tlsSecretName returns the TLS secret name for a cluster component.
func tlsSecretName(clusterName, component string) string {
	return fmt.Sprintf("%s-%s-tls", clusterName, component)
}

// tlsVolume creates a volume referencing a TLS secret.
func tlsVolume(secretName string) corev1.Volume {
	return corev1.Volume{
		Name: "tls",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secretName,
			},
		},
	}
}

// tlsVolumeMount creates a volume mount at /tls.
func tlsVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      "tls",
		MountPath: tlsMountPath,
		ReadOnly:  true,
	}
}

// tlsArgs returns the CLI flags for TLS certificate paths.
func tlsArgs() []string {
	return []string{
		fmt.Sprintf("--tls-cert=%s/tls.crt", tlsMountPath),
		fmt.Sprintf("--tls-key=%s/tls.key", tlsMountPath),
		fmt.Sprintf("--tls-ca=%s/ca.crt", tlsMountPath),
	}
}

// shardManagerURLForCluster returns the shard-manager URL, using https:// when TLS is enabled.
func shardManagerURLForCluster(cluster *tapirv1alpha1.TAPIRCluster) string {
	scheme := "http"
	if tlsEnabled(cluster) {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s-shard-manager.%s.svc.cluster.local:%d",
		scheme, cluster.Name, cluster.Namespace, shardManagerPort)
}

// desiredCertificate builds an unstructured cert-manager Certificate CR.
func desiredCertificate(cluster *tapirv1alpha1.TAPIRCluster, component string, dnsNames []string) *unstructured.Unstructured {
	secretName := tlsSecretName(cluster.Name, component)

	cert := &unstructured.Unstructured{}
	cert.SetAPIVersion(certManagerAPIVersion)
	cert.SetKind("Certificate")
	cert.SetName(secretName)
	cert.SetNamespace(cluster.Namespace)

	issuerKind := cluster.Spec.TLS.IssuerRef.Kind
	if issuerKind == "" {
		issuerKind = "ClusterIssuer"
	}

	cert.Object["spec"] = map[string]interface{}{
		"secretName": secretName,
		"issuerRef": map[string]interface{}{
			"name": cluster.Spec.TLS.IssuerRef.Name,
			"kind": issuerKind,
		},
		"dnsNames": dnsNames,
		"usages":   []interface{}{"server auth", "client auth"},
	}

	return cert
}

// injectTLS adds TLS volume, mount, and args to a container and volume list.
func injectTLS(container *corev1.Container, volumes *[]corev1.Volume, cluster *tapirv1alpha1.TAPIRCluster, component string) {
	if !tlsEnabled(cluster) {
		return
	}
	secretName := tlsSecretName(cluster.Name, component)
	*volumes = append(*volumes, tlsVolume(secretName))
	container.VolumeMounts = append(container.VolumeMounts, tlsVolumeMount())
	container.Args = append(container.Args, tlsArgs()...)
}

// reconcileTLS creates cert-manager Certificate CRs for all cluster components.
func (r *TAPIRClusterReconciler) reconcileTLS(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	if !tlsEnabled(cluster) {
		return nil
	}

	ns := cluster.Namespace

	// Discovery certificate
	discoverySvc := cluster.Name + "-discovery"
	if err := r.ensureCertificate(ctx, cluster, desiredCertificate(cluster, "discovery", []string{
		fmt.Sprintf("*.%s.%s.svc.cluster.local", discoverySvc, ns),
		fmt.Sprintf("%s.%s.svc.cluster.local", discoverySvc, ns),
	})); err != nil {
		return fmt.Errorf("discovery certificate: %w", err)
	}

	// Node pool certificates
	for _, pool := range cluster.Spec.NodePools {
		poolSvc := cluster.Name + "-" + pool.Name
		if err := r.ensureCertificate(ctx, cluster, desiredCertificate(cluster, pool.Name, []string{
			fmt.Sprintf("*.%s.%s.svc.cluster.local", poolSvc, ns),
			fmt.Sprintf("%s.%s.svc.cluster.local", poolSvc, ns),
		})); err != nil {
			return fmt.Errorf("node pool %s certificate: %w", pool.Name, err)
		}
	}

	// Shard-manager certificate
	smSvc := cluster.Name + "-shard-manager"
	if err := r.ensureCertificate(ctx, cluster, desiredCertificate(cluster, "shard-manager", []string{
		fmt.Sprintf("%s.%s.svc.cluster.local", smSvc, ns),
	})); err != nil {
		return fmt.Errorf("shard-manager certificate: %w", err)
	}

	// Operator client certificate (for admin/SM API calls from the reconciler)
	if err := r.ensureCertificate(ctx, cluster, desiredCertificate(cluster, "operator-client", []string{
		fmt.Sprintf("%s-operator-client.%s.svc.cluster.local", cluster.Name, ns),
	})); err != nil {
		return fmt.Errorf("operator-client certificate: %w", err)
	}

	return nil
}

// ensureCertificate creates or updates a cert-manager Certificate CR.
func (r *TAPIRClusterReconciler) ensureCertificate(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster, cert *unstructured.Unstructured) error {
	if err := controllerutil.SetControllerReference(cluster, cert, r.Scheme); err != nil {
		return err
	}

	var existing unstructured.Unstructured
	existing.SetGroupVersionKind(cert.GroupVersionKind())
	err := r.Get(ctx, types.NamespacedName{Name: cert.GetName(), Namespace: cert.GetNamespace()}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, cert)
	}
	if err != nil {
		return err
	}

	// Update if spec changed
	existingSpec, _ := existing.Object["spec"].(map[string]interface{})
	desiredSpec, _ := cert.Object["spec"].(map[string]interface{})
	if fmt.Sprintf("%v", existingSpec) != fmt.Sprintf("%v", desiredSpec) {
		existing.Object["spec"] = desiredSpec
		return r.Update(ctx, &existing)
	}
	return nil
}

// loadTLSConfigFromSecret reads the operator's client TLS config from a
// cert-manager-generated Secret. Returns nil if the Secret doesn't exist yet
// (cert-manager may not have issued the certificate).
func (r *TAPIRClusterReconciler) loadTLSConfigFromSecret(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) *tls.Config {
	secretName := tlsSecretName(cluster.Name, "operator-client")
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}, &secret); err != nil {
		return nil // Secret not ready yet
	}

	certPEM := secret.Data["tls.crt"]
	keyPEM := secret.Data["tls.key"]
	caPEM := secret.Data["ca.crt"]
	if len(certPEM) == 0 || len(keyPEM) == 0 || len(caPEM) == 0 {
		return nil
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}
}

// adminClient constructs an AdminClient with optional TLS for a pod address.
// When TLS is enabled, the address uses the pod's DNS FQDN instead of IP for
// hostname verification against the certificate's wildcard DNS SAN.
func (r *TAPIRClusterReconciler) adminClient(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster, p podInfo, serviceName string) *tapir.AdminClient {
	addr := fmt.Sprintf("%s:%d", p.PodIP, adminPort)
	client := &tapir.AdminClient{Addr: addr, Timeout: 10 * time.Second}

	if tlsEnabled(cluster) {
		// Use DNS FQDN for TLS hostname verification against wildcard SANs.
		addr = fmt.Sprintf("%s.%s.%s.svc.cluster.local:%d",
			p.Name, serviceName, cluster.Namespace, adminPort)
		client.Addr = addr
		client.TLSConfig = r.loadTLSConfigFromSecret(ctx, cluster)
	}

	return client
}

// shardManagerClient constructs a ShardManagerClient with optional TLS.
func (r *TAPIRClusterReconciler) shardManagerClient(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) *tapir.ShardManagerClient {
	smClient := &tapir.ShardManagerClient{
		BaseURL: shardManagerURLForCluster(cluster),
	}

	if tlsEnabled(cluster) {
		tlsConfig := r.loadTLSConfigFromSecret(ctx, cluster)
		if tlsConfig != nil {
			smClient.HTTPClient = &http.Client{
				Transport: &http.Transport{TLSClientConfig: tlsConfig},
				Timeout:   10 * time.Second,
			}
		}
	}

	return smClient
}
