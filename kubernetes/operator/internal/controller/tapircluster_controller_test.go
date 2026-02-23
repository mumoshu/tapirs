package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
)

var _ = Describe("TAPIRCluster Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		tapircluster := &tapirv1alpha1.TAPIRCluster{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind TAPIRCluster")
			err := k8sClient.Get(ctx, typeNamespacedName, tapircluster)
			if err != nil && errors.IsNotFound(err) {
				resource := &tapirv1alpha1.TAPIRCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
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
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &tapirv1alpha1.TAPIRCluster{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance TAPIRCluster")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &TAPIRClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
