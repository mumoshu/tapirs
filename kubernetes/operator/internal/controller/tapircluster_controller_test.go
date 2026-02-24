package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
)

const tlsVolumeName = "tls"

var _ = Describe("TAPIRCluster TLS resource generation", func() {
	tlsCluster := &tapirv1alpha1.TAPIRCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-test",
			Namespace: "default",
		},
		Spec: tapirv1alpha1.TAPIRClusterSpec{
			Image: "tapir:latest",
			Discovery: tapirv1alpha1.DiscoverySpec{
				Replicas: 3,
			},
			NodePools: []tapirv1alpha1.NodePool{
				{Name: "default", Replicas: 3},
			},
			Shards: []tapirv1alpha1.ShardSpec{
				{Number: 0, Replicas: 3, KeyRangeEnd: "n"},
				{Number: 1, Replicas: 3, KeyRangeStart: "n"},
			},
			TLS: &tapirv1alpha1.TLSSpec{
				Enabled: true,
				IssuerRef: &tapirv1alpha1.IssuerRef{
					Name: "test-issuer",
					Kind: "ClusterIssuer",
				},
			},
		},
	}

	It("should inject TLS volume and args into discovery StatefulSet", func() {
		sts := desiredDiscoveryStatefulSet(tlsCluster)
		container := sts.Spec.Template.Spec.Containers[0]
		volumes := sts.Spec.Template.Spec.Volumes

		By("verifying TLS volume exists")
		var foundTLSVolume bool
		for _, v := range volumes {
			if v.Name == tlsVolumeName && v.Secret != nil {
				Expect(v.Secret.SecretName).To(Equal("tls-test-discovery-tls"))
				foundTLSVolume = true
			}
		}
		Expect(foundTLSVolume).To(BeTrue(), "TLS volume not found")

		By("verifying TLS volume mount exists")
		var foundTLSMount bool
		for _, m := range container.VolumeMounts {
			if m.Name == tlsVolumeName && m.MountPath == "/tls" {
				Expect(m.ReadOnly).To(BeTrue())
				foundTLSMount = true
			}
		}
		Expect(foundTLSMount).To(BeTrue(), "TLS volume mount not found")

		By("verifying TLS CLI flags in container args")
		Expect(container.Args).To(ContainElement("--tls-cert=/tls/tls.crt"))
		Expect(container.Args).To(ContainElement("--tls-key=/tls/tls.key"))
		Expect(container.Args).To(ContainElement("--tls-ca=/tls/ca.crt"))
	})

	It("should inject TLS volume and args into shard-manager Deployment", func() {
		deploy := desiredShardManagerDeployment(tlsCluster)
		container := deploy.Spec.Template.Spec.Containers[0]
		volumes := deploy.Spec.Template.Spec.Volumes

		By("verifying TLS volume exists")
		var foundTLSVolume bool
		for _, v := range volumes {
			if v.Name == tlsVolumeName && v.Secret != nil {
				Expect(v.Secret.SecretName).To(Equal("tls-test-shard-manager-tls"))
				foundTLSVolume = true
			}
		}
		Expect(foundTLSVolume).To(BeTrue(), "TLS volume not found")

		By("verifying TLS CLI flags in container args")
		Expect(container.Args).To(ContainElement("--tls-cert=/tls/tls.crt"))
		Expect(container.Args).To(ContainElement("--tls-key=/tls/tls.key"))
		Expect(container.Args).To(ContainElement("--tls-ca=/tls/ca.crt"))

		By("verifying readiness probe uses exec healthcheck for TLS")
		probe := container.ReadinessProbe
		Expect(probe).NotTo(BeNil())
		Expect(probe.Exec).NotTo(BeNil())
		Expect(probe.Exec.Command[0]).To(Equal("tapi"))
		Expect(probe.Exec.Command[1]).To(Equal("shard-manager-healthz"))
	})

	It("should inject TLS volume and args into node pool StatefulSet", func() {
		pool := tapirv1alpha1.NodePool{Name: "default", Replicas: 3}
		sts := desiredNodePoolStatefulSet(tlsCluster, pool)
		container := sts.Spec.Template.Spec.Containers[0]
		volumes := sts.Spec.Template.Spec.Volumes

		By("verifying TLS volume exists")
		var foundTLSVolume bool
		for _, v := range volumes {
			if v.Name == tlsVolumeName && v.Secret != nil {
				Expect(v.Secret.SecretName).To(Equal("tls-test-default-tls"))
				foundTLSVolume = true
			}
		}
		Expect(foundTLSVolume).To(BeTrue(), "TLS volume not found")

		By("verifying TLS CLI flags in container args")
		Expect(container.Args).To(ContainElement("--tls-cert=/tls/tls.crt"))
		Expect(container.Args).To(ContainElement("--tls-key=/tls/tls.key"))
		Expect(container.Args).To(ContainElement("--tls-ca=/tls/ca.crt"))
	})

	It("should use https:// for shard-manager URL when TLS is enabled", func() {
		url := shardManagerURLForCluster(tlsCluster)
		Expect(url).To(HavePrefix("https://"))
		Expect(url).To(ContainSubstring("tls-test-shard-manager"))
	})

	It("should use http:// for shard-manager URL when TLS is disabled", func() {
		plainCluster := &tapirv1alpha1.TAPIRCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "default"},
			Spec:       tapirv1alpha1.TAPIRClusterSpec{Image: "tapir:latest"},
		}
		url := shardManagerURLForCluster(plainCluster)
		Expect(url).To(HavePrefix("http://"))
	})

	It("should not inject TLS when disabled", func() {
		plainCluster := &tapirv1alpha1.TAPIRCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "default"},
			Spec: tapirv1alpha1.TAPIRClusterSpec{
				Image:     "tapir:latest",
				Discovery: tapirv1alpha1.DiscoverySpec{Replicas: 3},
			},
		}
		sts := desiredDiscoveryStatefulSet(plainCluster)
		container := sts.Spec.Template.Spec.Containers[0]

		for _, v := range sts.Spec.Template.Spec.Volumes {
			Expect(v.Name).NotTo(Equal(tlsVolumeName), "TLS volume should not exist")
		}
		for _, m := range container.VolumeMounts {
			Expect(m.Name).NotTo(Equal(tlsVolumeName), "TLS mount should not exist")
		}
		for _, arg := range container.Args {
			Expect(arg).NotTo(ContainSubstring("--tls-"), "TLS args should not exist")
		}
	})

	It("should build correct cert-manager Certificate for discovery", func() {
		cert := desiredCertificate(tlsCluster, "discovery", []string{
			"*.tls-test-discovery.default.svc.cluster.local",
			"tls-test-discovery.default.svc.cluster.local",
		})
		Expect(cert.GetKind()).To(Equal("Certificate"))
		Expect(cert.GetAPIVersion()).To(Equal("cert-manager.io/v1"))
		Expect(cert.GetName()).To(Equal("tls-test-discovery-tls"))

		spec := cert.Object["spec"].(map[string]any)
		Expect(spec["secretName"]).To(Equal("tls-test-discovery-tls"))

		issuerRef := spec["issuerRef"].(map[string]any)
		Expect(issuerRef["name"]).To(Equal("test-issuer"))
		Expect(issuerRef["kind"]).To(Equal("ClusterIssuer"))

		dnsNames := spec["dnsNames"].([]string)
		Expect(dnsNames).To(HaveLen(2))
		Expect(dnsNames[0]).To(Equal("*.tls-test-discovery.default.svc.cluster.local"))
	})
})

var _ = Describe("TAPIRCluster Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"
		const ns = "default"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: ns,
		}

		BeforeEach(func() {
			By("creating the custom resource for TAPIRCluster")
			tapircluster := &tapirv1alpha1.TAPIRCluster{}
			err := k8sClient.Get(ctx, typeNamespacedName, tapircluster)
			if err != nil && errors.IsNotFound(err) {
				resource := &tapirv1alpha1.TAPIRCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: ns,
					},
					Spec: tapirv1alpha1.TAPIRClusterSpec{
						Image: "tapir:latest",
						Discovery: tapirv1alpha1.DiscoverySpec{
							Replicas: 3,
						},
						NodePools: []tapirv1alpha1.NodePool{
							{Name: "default", Replicas: 3},
						},
						Shards: []tapirv1alpha1.ShardSpec{
							{Number: 0, Replicas: 3, KeyRangeEnd: "n"},
							{Number: 1, Replicas: 3, KeyRangeStart: "n"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// Clean up sub-resources first (envtest doesn't garbage-collect owner refs)
			for _, name := range []string{
				resourceName + "-discovery",
				resourceName + "-default",
			} {
				sts := &appsv1.StatefulSet{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, sts); err == nil {
					_ = k8sClient.Delete(ctx, sts)
				}
				svc := &corev1.Service{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, svc); err == nil {
					_ = k8sClient.Delete(ctx, svc)
				}
			}
			smDeploy := &appsv1.Deployment{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-shard-manager", Namespace: ns}, smDeploy); err == nil {
				_ = k8sClient.Delete(ctx, smDeploy)
			}
			smSvc := &corev1.Service{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-shard-manager", Namespace: ns}, smSvc); err == nil {
				_ = k8sClient.Delete(ctx, smSvc)
			}

			resource := &tapirv1alpha1.TAPIRCluster{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Cleanup the TAPIRCluster instance")
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should create discovery StatefulSet and headless Service", func() {
			controllerReconciler := &TAPIRClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the discovery headless Service was created")
			var discoverySvc corev1.Service
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-discovery", Namespace: ns,
			}, &discoverySvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(discoverySvc.Spec.ClusterIP).To(Equal("None"))
			Expect(discoverySvc.Spec.Ports).To(HaveLen(2))

			By("verifying the discovery StatefulSet was created")
			var discoverySts appsv1.StatefulSet
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-discovery", Namespace: ns,
			}, &discoverySts)
			Expect(err).NotTo(HaveOccurred())
			Expect(*discoverySts.Spec.Replicas).To(Equal(int32(3)))
			Expect(discoverySts.Spec.ServiceName).To(Equal(resourceName + "-discovery"))
			Expect(discoverySts.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(discoverySts.Spec.Template.Spec.Containers[0].Image).To(Equal("tapir:latest"))
			Expect(discoverySts.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"tapi", "node"}))
		})

		It("should create shard-manager Deployment and ClusterIP Service", func() {
			// Advance phase past discovery bootstrap so shard-manager is created.
			var cluster tapirv1alpha1.TAPIRCluster
			Expect(k8sClient.Get(ctx, typeNamespacedName, &cluster)).To(Succeed())
			cluster.Status.Phase = tapirv1alpha1.PhaseCreatingDataPlane
			Expect(k8sClient.Status().Update(ctx, &cluster)).To(Succeed())

			controllerReconciler := &TAPIRClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the shard-manager Service was created")
			var smSvc corev1.Service
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-shard-manager", Namespace: ns,
			}, &smSvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(smSvc.Spec.ClusterIP).NotTo(Equal("None"))
			Expect(smSvc.Spec.Ports).To(HaveLen(1))
			Expect(smSvc.Spec.Ports[0].Port).To(Equal(int32(9001)))

			By("verifying the shard-manager Deployment was created")
			var smDeploy appsv1.Deployment
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-shard-manager", Namespace: ns,
			}, &smDeploy)
			Expect(err).NotTo(HaveOccurred())
			Expect(*smDeploy.Spec.Replicas).To(Equal(int32(1)))
			Expect(smDeploy.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(smDeploy.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"tapi", "shard-manager"}))
			// Verify it references the discovery endpoint
			Expect(smDeploy.Spec.Template.Spec.Containers[0].Args).To(ContainElement(
				ContainSubstring("--discovery-tapir-endpoint=srv://")))

			By("verifying readiness probe uses HTTP /healthz")
			probe := smDeploy.Spec.Template.Spec.Containers[0].ReadinessProbe
			Expect(probe).NotTo(BeNil())
			Expect(probe.HTTPGet).NotTo(BeNil())
			Expect(probe.HTTPGet.Path).To(Equal("/healthz"))
		})

		It("should create node pool StatefulSet and headless Service", func() {
			// Advance phase past discovery bootstrap so node pools are created.
			var cluster tapirv1alpha1.TAPIRCluster
			Expect(k8sClient.Get(ctx, typeNamespacedName, &cluster)).To(Succeed())
			cluster.Status.Phase = tapirv1alpha1.PhaseCreatingDataPlane
			Expect(k8sClient.Status().Update(ctx, &cluster)).To(Succeed())

			controllerReconciler := &TAPIRClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the node pool headless Service was created")
			var nodeSvc corev1.Service
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-default", Namespace: ns,
			}, &nodeSvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(nodeSvc.Spec.ClusterIP).To(Equal("None"))
			// admin + 2 shard ports
			Expect(nodeSvc.Spec.Ports).To(HaveLen(3))

			By("verifying the node pool StatefulSet was created")
			var nodeSts appsv1.StatefulSet
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-default", Namespace: ns,
			}, &nodeSts)
			Expect(err).NotTo(HaveOccurred())
			Expect(*nodeSts.Spec.Replicas).To(Equal(int32(3)))
			Expect(nodeSts.Spec.ServiceName).To(Equal(resourceName + "-default"))
			Expect(nodeSts.Spec.Template.Spec.Containers).To(HaveLen(1))
			container := nodeSts.Spec.Template.Spec.Containers[0]
			Expect(container.Command).To(Equal([]string{"tapi", "node"}))
			// admin + 2 shard ports
			Expect(container.Ports).To(HaveLen(3))
			// Verify it references discovery endpoint and shard-manager URL
			Expect(container.Args).To(ContainElement(ContainSubstring("--discovery-tapir-endpoint=")))
			Expect(container.Args).To(ContainElement(ContainSubstring("--shard-manager-url=")))
		})

		It("should set status phase and endpoints after reconcile", func() {
			controllerReconciler := &TAPIRClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("verifying the status was updated")
			var cluster tapirv1alpha1.TAPIRCluster
			err = k8sClient.Get(ctx, typeNamespacedName, &cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Status.Phase).NotTo(BeEmpty())
			Expect(cluster.Status.DiscoveryEndpoint).To(ContainSubstring("srv://"))
			Expect(cluster.Status.DiscoveryEndpoint).To(ContainSubstring(resourceName + "-discovery"))
			Expect(cluster.Status.ShardManagerURL).To(ContainSubstring("http://"))
			Expect(cluster.Status.ShardManagerURL).To(ContainSubstring(resourceName + "-shard-manager"))
		})

		It("should set owner references on created resources", func() {
			// Advance phase past discovery bootstrap so all resources are created.
			var clusterObj tapirv1alpha1.TAPIRCluster
			Expect(k8sClient.Get(ctx, typeNamespacedName, &clusterObj)).To(Succeed())
			clusterObj.Status.Phase = tapirv1alpha1.PhaseCreatingDataPlane
			Expect(k8sClient.Status().Update(ctx, &clusterObj)).To(Succeed())

			controllerReconciler := &TAPIRClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Get the CR to check its UID
			var cluster tapirv1alpha1.TAPIRCluster
			Expect(k8sClient.Get(ctx, typeNamespacedName, &cluster)).To(Succeed())

			By("verifying discovery StatefulSet has owner reference")
			var discoverySts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-discovery", Namespace: ns,
			}, &discoverySts)).To(Succeed())
			Expect(discoverySts.OwnerReferences).To(HaveLen(1))
			Expect(discoverySts.OwnerReferences[0].UID).To(Equal(cluster.UID))

			By("verifying shard-manager Deployment has owner reference")
			var smDeploy appsv1.Deployment
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-shard-manager", Namespace: ns,
			}, &smDeploy)).To(Succeed())
			Expect(smDeploy.OwnerReferences).To(HaveLen(1))
			Expect(smDeploy.OwnerReferences[0].UID).To(Equal(cluster.UID))
		})

		It("should be idempotent on multiple reconciles", func() {
			controllerReconciler := &TAPIRClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			// First reconcile
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Get resource versions after first reconcile
			var sts1 appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-discovery", Namespace: ns,
			}, &sts1)).To(Succeed())

			// Second reconcile
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify the StatefulSet still exists and spec hasn't been corrupted
			var sts2 appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: resourceName + "-discovery", Namespace: ns,
			}, &sts2)).To(Succeed())
			Expect(*sts2.Spec.Replicas).To(Equal(int32(3)))
		})
	})
})
