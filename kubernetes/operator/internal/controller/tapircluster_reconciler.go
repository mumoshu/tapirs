package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	tapirv1alpha1 "github.com/mumoshu/tapirs/kubernetes/operator/api/v1alpha1"
)

// TAPIRClusterReconciler reconciles a TAPIRCluster object
type TAPIRClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=tapir.tapir.dev,resources=tapirclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tapir.tapir.dev,resources=tapirclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tapir.tapir.dev,resources=tapirclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile creates or updates the Kubernetes resources that form a tapirs cluster,
// then orchestrates bootstrap and scaling operations.
func (r *TAPIRClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cluster tapirv1alpha1.TAPIRCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Phase: ensure resources exist
	if cluster.Status.Phase == "" || cluster.Status.Phase == tapirv1alpha1.PhasePending {
		cluster.Status.Phase = tapirv1alpha1.PhaseCreatingDiscovery
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Create or update all sub-resources (StatefulSets, Deployments, Services).
	// Resources are always declared regardless of bootstrap phase. The bootstrap
	// state machine in reconcileBootstrap gates only operational steps (admin API
	// calls, shard registration) behind the appropriate phases.
	if err := r.reconcileDiscovery(ctx, &cluster); err != nil {
		log.Error(err, "Failed to reconcile discovery resources")
		return ctrl.Result{}, err
	}

	if err := r.reconcileShardManager(ctx, &cluster); err != nil {
		log.Error(err, "Failed to reconcile shard-manager resources")
		return ctrl.Result{}, err
	}

	if err := r.reconcileNodePools(ctx, &cluster); err != nil {
		log.Error(err, "Failed to reconcile node pool resources")
		return ctrl.Result{}, err
	}

	// Update status endpoints
	cluster.Status.DiscoveryEndpoint = discoveryEndpoint(&cluster)
	cluster.Status.ShardManagerURL = shardManagerURL(&cluster)
	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}

	// Drive bootstrap state machine
	requeue, err := r.reconcileBootstrap(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if requeue {
		return ctrl.Result{RequeueAfter: requeueDelay}, nil
	}

	return ctrl.Result{}, nil
}

func (r *TAPIRClusterReconciler) reconcileDiscovery(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	// Service
	svc := desiredDiscoveryService(cluster)
	if err := r.createOrUpdateService(ctx, cluster, svc); err != nil {
		return fmt.Errorf("discovery service: %w", err)
	}

	// StatefulSet
	sts := desiredDiscoveryStatefulSet(cluster)
	if err := r.createOrUpdateStatefulSet(ctx, cluster, sts); err != nil {
		return fmt.Errorf("discovery statefulset: %w", err)
	}

	return nil
}

func (r *TAPIRClusterReconciler) reconcileShardManager(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	// Service
	svc := desiredShardManagerService(cluster)
	if err := r.createOrUpdateService(ctx, cluster, svc); err != nil {
		return fmt.Errorf("shard-manager service: %w", err)
	}

	// Deployment
	deploy := desiredShardManagerDeployment(cluster)
	if err := r.createOrUpdateDeployment(ctx, cluster, deploy); err != nil {
		return fmt.Errorf("shard-manager deployment: %w", err)
	}

	return nil
}

func (r *TAPIRClusterReconciler) reconcileNodePools(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster) error {
	for _, pool := range cluster.Spec.NodePools {
		svc := desiredNodePoolService(cluster, pool)
		if err := r.createOrUpdateService(ctx, cluster, svc); err != nil {
			return fmt.Errorf("node pool %s service: %w", pool.Name, err)
		}

		sts := desiredNodePoolStatefulSet(cluster, pool)
		if err := r.createOrUpdateStatefulSet(ctx, cluster, sts); err != nil {
			return fmt.Errorf("node pool %s statefulset: %w", pool.Name, err)
		}
	}
	return nil
}

func (r *TAPIRClusterReconciler) createOrUpdateService(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster, desired *corev1.Service) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}

	var existing corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Preserve ClusterIP (immutable field)
	desired.Spec.ClusterIP = existing.Spec.ClusterIP
	if !equality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) ||
		!equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		existing.Spec.Ports = desired.Spec.Ports
		existing.Spec.Selector = desired.Spec.Selector
		return r.Update(ctx, &existing)
	}
	return nil
}

func (r *TAPIRClusterReconciler) createOrUpdateStatefulSet(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster, desired *appsv1.StatefulSet) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}

	var existing appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update mutable fields only if changed
	if !equality.Semantic.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) ||
		!equality.Semantic.DeepEqual(existing.Spec.Template, desired.Spec.Template) {
		existing.Spec.Replicas = desired.Spec.Replicas
		existing.Spec.Template = desired.Spec.Template
		return r.Update(ctx, &existing)
	}
	return nil
}

func (r *TAPIRClusterReconciler) createOrUpdateDeployment(ctx context.Context, cluster *tapirv1alpha1.TAPIRCluster, desired *appsv1.Deployment) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}

	var existing appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update mutable fields only if changed
	if !equality.Semantic.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) ||
		!equality.Semantic.DeepEqual(existing.Spec.Template, desired.Spec.Template) {
		existing.Spec.Replicas = desired.Spec.Replicas
		existing.Spec.Template = desired.Spec.Template
		return r.Update(ctx, &existing)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TAPIRClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tapirv1alpha1.TAPIRCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Named("tapircluster").
		Complete(r)
}
