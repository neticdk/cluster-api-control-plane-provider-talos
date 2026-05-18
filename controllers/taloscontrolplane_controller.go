// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/certs"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/predicates"
	"sigs.k8s.io/cluster-api/util/secret"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

const requeueDuration = 30 * time.Second

// TalosControlPlaneReconciler reconciles a TalosControlPlane object
type TalosControlPlaneReconciler struct {
	client.Client
	APIReader    client.Reader
	Log          logr.Logger
	Scheme       *runtime.Scheme
	ClusterCache clustercache.ClusterCache
}

func (r *TalosControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	predicateLog := ctrl.Log.WithValues("controller", "taloscontrolplane")
	return ctrl.NewControllerManagedBy(mgr).
		For(&controlplanev1.TalosControlPlane{}).
		Owns(&clusterv1.Machine{}).
		Watches(
			&clusterv1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.ClusterToTalosControlPlane),
			builder.WithPredicates(predicates.ClusterPausedTransitionsOrInfrastructureProvisioned(mgr.GetScheme(), predicateLog)),
		).
		WithOptions(options).
		Complete(r)
}

// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;patch;update
// +kubebuilder:rbac:groups=core,resources=configmaps,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=roles,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=rolebindings,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io;bootstrap.cluster.x-k8s.io;controlplane.cluster.x-k8s.io,resources=*,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;clusters/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machines/status,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

func (r *TalosControlPlaneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, reterr error) {
	logger := r.Log.WithValues("namespace", req.Namespace, "talosControlPlane", req.Name)

	// Fetch the TalosControlPlane instance.
	tcp := &controlplanev1.TalosControlPlane{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, tcp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Fetch the Cluster.
	cluster, err := util.GetOwnerCluster(ctx, r.Client, tcp.ObjectMeta)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to retrieve owner Cluster from the API Server")

			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	if cluster == nil {
		logger.Info("cluster Controller has not yet set OwnerRef")
		return ctrl.Result{Requeue: true}, nil
	}
	logger = logger.WithValues("cluster", cluster.Name)

	if annotations.IsPaused(cluster, tcp) {
		logger.Info("reconciliation is paused for this object")
		return ctrl.Result{Requeue: true}, nil
	}

	// Wait for the cluster infrastructure to be ready before creating machines
	if !ptr.Deref(cluster.Status.Initialization.InfrastructureProvisioned, false) {
		logger.Info("cluster infrastructure is not ready yet")

		return ctrl.Result{}, nil
	}

	// Initialize the patch helper.
	patchHelper, err := patch.NewHelper(tcp, r.Client)
	if err != nil {
		logger.Error(err, "failed to configure the patch helper")
		return ctrl.Result{Requeue: true}, nil
	}

	// Add finalizer first if not exist to avoid the race condition between init and delete
	if ensureTalosControlPlaneFinalizers(tcp) {

		// patch and return right away instead of reusing the main defer,
		// because the main defer may take too much time to get cluster status

		if err := patchTalosControlPlane(ctx, patchHelper, tcp, patch.WithStatusObservedGeneration{}); err != nil {
			logger.Error(err, "failed to add finalizer to TalosControlPlane")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	defer func() {
		r.Log.Info("attempting to set control plane status")

		// Always attempt to update status.
		if err := r.updateStatus(ctx, tcp, cluster); err != nil {
			logger.Error(err, "failed to update TalosControlPlane Status")

			reterr = kerrors.NewAggregate([]error{reterr, err})
		}

		// Always attempt to Patch the TalosControlPlane object and status after each reconciliation.
		if err := patchTalosControlPlane(ctx, patchHelper, tcp, patch.WithStatusObservedGeneration{}); err != nil {
			logger.Error(err, "failed to patch TalosControlPlane")
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}

		// TODO: remove this as soon as we have a proper remote cluster cache in place.
		// Make TCP to requeue in case status is not ready, so we can check for node status without waiting for a full resync (by default 10 minutes).
		// Only requeue if we are not going in exponential backoff due to error, or if we are not already re-queueing, or if the object has a deletion timestamp.
		if reterr == nil && !res.Requeue && res.RequeueAfter <= 0 && tcp.ObjectMeta.DeletionTimestamp.IsZero() {
			deprecated := tcp.V1Beta1DeprecatedStatus()
			if !deprecated.Ready || deprecated.UnavailableReplicas > 0 {
				res = ctrl.Result{RequeueAfter: 20 * time.Second}
			}
		}

		logger.Info("successfully updated control plane status")
	}()

	if !tcp.ObjectMeta.DeletionTimestamp.IsZero() {
		// Handle deletion reconciliation loop.
		return r.reconcileDelete(ctx, cluster, tcp)
	}

	return r.reconcile(ctx, cluster, tcp)
}

func (r *TalosControlPlaneReconciler) reconcile(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (res ctrl.Result, err error) {
	logger := ctrl.LoggerFrom(ctx, "cluster", cluster.Name)
	logger.Info("reconcile TalosControlPlane")

	// Update ownerrefs on infra templates
	if err := r.reconcileExternalReference(ctx, tcp.Spec.MachineTemplate.Spec.InfrastructureRef, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// If ControlPlaneEndpoint is not set, return early
	if !cluster.Spec.ControlPlaneEndpoint.IsValid() {
		logger.Info("cluster does not yet have a ControlPlaneEndpoint defined")

		return ctrl.Result{}, nil
	}

	// TODO: handle proper adoption of Machines
	ownedMachines, err := r.getControlPlaneMachinesForCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		logger.Error(err, "failed to retrieve control plane machines for cluster")

		return ctrl.Result{}, err
	}

	controlPlane, err := newControlPlane(ctx, r.Client, cluster, tcp, collections.FromMachineList(&ownedMachines))
	if err != nil {
		logger.Error(err, "failed to initialize control plane logic object")

		return ctrl.Result{}, err
	}

	var (
		errs        error
		result      ctrl.Result
		phaseResult ctrl.Result
	)

	// run all similar reconcile steps in the loop and pick the lowest RetryAfter, aggregate errors and check the requeue flags.
	for _, phase := range []func(context.Context, *ControlPlane) (ctrl.Result, error){
		r.reconcileMachineConditions,
		r.reconcileEtcdMembers,
		r.reconcileNodeHealth,
		r.reconcileConditions,
		r.reconcileKubeconfig,
		r.reconcileMachines,
	} {
		phaseResult, err = phase(ctx, controlPlane)
		if err != nil {
			errs = kerrors.NewAggregate([]error{errs, err})
		}

		result = util.LowestNonZeroResult(result, phaseResult)
	}

	if result.RequeueAfter != 0 {
		if err != nil {
			r.Log.Error(err, "reconcile failed", "requeue after", result.RequeueAfter.String(), "error", err.Error())
		}

		return result, nil
	}

	return result, errs
}

// ClusterToTalosControlPlane is a handler.ToRequestsFunc to be used to enqueue requests for reconciliation
// for TalosControlPlane based on updates to a Cluster.
func (r *TalosControlPlaneReconciler) ClusterToTalosControlPlane(_ context.Context, o client.Object) []ctrl.Request {
	c, ok := o.(*clusterv1.Cluster)
	if !ok {
		r.Log.Error(nil, fmt.Sprintf("expected a Cluster but got a %T", o))
		return nil
	}

	controlPlaneRef := c.Spec.ControlPlaneRef
	if controlPlaneRef.IsDefined() && controlPlaneRef.Kind == "TalosControlPlane" {
		return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: c.Namespace, Name: controlPlaneRef.Name}}}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileDelete(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (ctrl.Result, error) {
	// Get list of all control plane machines
	ownedMachines, err := r.getControlPlaneMachinesForCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		r.Log.Error(err, "failed to retrieve control plane machines for cluster")

		return ctrl.Result{}, err
	}

	// If no control plane machines remain, remove the finalizer
	if len(ownedMachines.Items) == 0 {
		removeTalosControlPlaneFinalizers(tcp)
		return ctrl.Result{}, r.Client.Update(ctx, tcp)
	}

	for _, ownedMachine := range ownedMachines.Items {
		// Already deleting this machine
		if !ownedMachine.ObjectMeta.DeletionTimestamp.IsZero() {
			continue
		}
		// Submit deletion request
		if err := r.Client.Delete(ctx, &ownedMachine); err != nil && !apierrors.IsNotFound(err) {
			r.Log.Error(err, "failed to cleanup owned machine")
			return ctrl.Result{}, err
		}
	}

	conditions.Set(tcp, metav1.Condition{
		Type:   string(controlplanev1.ResizedCondition),
		Status: metav1.ConditionFalse,
		Reason: clusterv1.DeletingReason,
	})
	// Requeue the deletion so we can check to make sure machines got cleaned up
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

func (r *TalosControlPlaneReconciler) getControlPlaneMachinesForCluster(ctx context.Context, cluster client.ObjectKey) (clusterv1.MachineList, error) {
	selector := map[string]string{
		clusterv1.ClusterNameLabel:         cluster.Name,
		clusterv1.MachineControlPlaneLabel: "",
	}

	machineList := clusterv1.MachineList{}
	if err := r.Client.List(
		ctx,
		&machineList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return machineList, err
	}

	return machineList, nil
}

func ensureTalosControlPlaneFinalizers(tcp *controlplanev1.TalosControlPlane) bool {
	changed := false

	if !controllerutil.ContainsFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer) {
		controllerutil.AddFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer)
		changed = true
	}

	if controllerutil.ContainsFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy) {
		controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy)
		changed = true
	}

	return changed
}

func removeTalosControlPlaneFinalizers(tcp *controlplanev1.TalosControlPlane) {
	controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer)
	controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy)
}

func (r *TalosControlPlaneReconciler) bootControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, controlPlane *ControlPlane, first bool) (ctrl.Result, error) {
	// Since the cloned resource should eventually have a controller ref for the Machine, we create an
	// OwnerReference here without the Controller field set
	infraCloneOwner := &metav1.OwnerReference{
		APIVersion: controlplanev1.GroupVersion.String(),
		Kind:       "TalosControlPlane",
		Name:       tcp.Name,
		UID:        tcp.UID,
	}

	machineName, err := tcp.Spec.GenerateMachineName(cluster.Name, tcp.Name)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.MachineGenerationFailedReason,
			Message: fmt.Sprintf("Failed to generate machine name: %v", err),
		})

		return ctrl.Result{}, err
	}

	// Clone the infrastructure template. The infrastructureRef is a CAPI v1beta2
	// ContractVersionedObjectReference, so we resolve the concrete APIVersion via
	// contract labels before delegating to external.CreateFromTemplate.
	templateObj, err := external.GetObjectFromContractVersionedRef(ctx, r.Client, tcp.Spec.MachineTemplate.Spec.InfrastructureRef, tcp.Namespace)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InfrastructureTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to retrieve infrastructure template: %v", err),
		})

		// A missing template is a user-recoverable misconfiguration; back off instead of
		// returning the error so the controller does not hot-loop at MaxConcurrentReconciles.
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		return ctrl.Result{}, err
	}

	templateRef := corev1.ObjectReference{
		APIVersion: templateObj.GetAPIVersion(),
		Kind:       templateObj.GetKind(),
		Name:       templateObj.GetName(),
		Namespace:  templateObj.GetNamespace(),
	}
	_, infraRef, err := external.CreateFromTemplate(ctx, &external.CreateFromTemplateInput{
		Client:      r.Client,
		TemplateRef: &templateRef,
		Namespace:   tcp.Namespace,
		Name:        machineName,
		OwnerRef:    infraCloneOwner,
		ClusterName: cluster.Name,
		Labels: map[string]string{
			clusterv1.MachineControlPlaneLabel: "",
		},
	})
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InfrastructureTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to clone infrastructure template: %v", err),
		})

		return ctrl.Result{}, err
	}

	bootstrapConfig := &tcp.Spec.ControlPlaneConfig.ControlPlaneConfig
	if !reflect.ValueOf(tcp.Spec.ControlPlaneConfig.InitConfig).IsZero() && first {
		bootstrapConfig = &tcp.Spec.ControlPlaneConfig.InitConfig
	}

	// Clone the bootstrap configuration
	bootstrapRef, err := r.generateTalosConfig(ctx, tcp, machineName, bootstrapConfig)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.BootstrapTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to create bootstrap configuration: %v", err),
		})

		return ctrl.Result{}, err
	}

	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      machineName,
			Namespace: tcp.Namespace,
			Labels:    controlPlaneMachineLabelsForCluster(tcp, cluster.Name),
			Annotations: copyStringMap(
				tcp.Spec.MachineTemplate.ObjectMeta.Annotations,
			),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane")),
			},
		},
		Spec: clusterv1.MachineSpec{
			ClusterName:       cluster.Name,
			Version:           tcp.Spec.Version,
			InfrastructureRef: infraRef,
			ReadinessGates:    copyMachineReadinessGates(tcp.Spec.MachineTemplate.Spec.ReadinessGates),
			Deletion:          buildMachineDeletionSpec(tcp.Spec.MachineTemplate.Spec.Deletion),
			Bootstrap: clusterv1.Bootstrap{
				ConfigRef: *bootstrapRef,
			},
		},
	}

	fd, err := controlPlane.NextFailureDomainForScaleUp(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if fd != "" {
		machine.Spec.FailureDomain = fd
	}

	if err := r.Client.Create(ctx, machine); err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.MachineGenerationFailedReason,
			Message: fmt.Sprintf("Failed to create machine: %v", err),
		})

		return ctrl.Result{}, errors.Wrap(err, "Failed to create machine")
	}

	return ctrl.Result{Requeue: true}, nil
}

func (r *TalosControlPlaneReconciler) bootstrapCluster(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []*clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	machineSlice := make([]clusterv1.Machine, 0, len(machines))
	for _, m := range machines {
		machineSlice = append(machineSlice, *m)
	}

	c, err := r.talosconfigForMachines(ctx, tcp, machineSlice...)
	if err != nil {
		return err
	}

	defer c.Close() //nolint:errcheck

	addresses := []string{}
	for _, machine := range machines {
		found := false

		// Prefer finding an InternalIP address for the machine first.
		for _, addr := range machine.Status.Addresses {
			if addr.Type == clusterv1.MachineInternalIP {
				addresses = append(addresses, addr.Address)

				found = true

				break
			}
		}

		if found {
			continue
		}

		// Fallback to finding an ExternalIP address for the machine
		// if no InternalIP is found.
		for _, addr := range machine.Status.Addresses {
			if addr.Type == clusterv1.MachineExternalIP {
				addresses = append(addresses, addr.Address)

				found = true

				break
			}
		}

		if !found {
			return fmt.Errorf("machine %q doesn't have an any InternalIP or ExternalIP address yet", machine.Name)
		}
	}

	if len(addresses) == 0 {
		return fmt.Errorf("no machine addresses to use for bootstrap")
	}

	list, err := c.LS(talosclient.WithNodes(ctx, addresses...), &machineapi.ListRequest{Root: "/var/lib/etcd/member"})
	if err != nil {
		return err
	}

	for {
		info, err := list.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || talosclient.StatusCode(err) == codes.Canceled {
				break
			}

			return err
		}

		// if the directory exists at least on a single node it means that cluster
		// was already bootstrapped
		if info.Metadata.Error == "" {
			return nil
		}
	}

	sort.Strings(addresses)

	if err := c.Bootstrap(talosclient.WithNodes(ctx, addresses[0]), &machineapi.BootstrapRequest{}); err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return err
		}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) generateTalosConfig(ctx context.Context, tcp *controlplanev1.TalosControlPlane, name string, spec *cabptv1.TalosConfigSpec) (*clusterv1.ContractVersionedObjectReference, error) {
	owner := metav1.OwnerReference{
		APIVersion:         controlplanev1.GroupVersion.String(),
		Kind:               "TalosControlPlane",
		Name:               tcp.Name,
		UID:                tcp.UID,
		BlockOwnerDeletion: ptr.To(true),
	}

	bootstrapConfig := &cabptv1.TalosConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       tcp.Namespace,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: *spec,
	}

	if err := r.Client.Create(ctx, bootstrapConfig); err != nil {
		return nil, errors.Wrap(err, "Failed to create bootstrap configuration")
	}

	bootstrapRef := &clusterv1.ContractVersionedObjectReference{
		APIGroup: cabptv1.GroupVersion.Group,
		Kind:     "TalosConfig",
		Name:     bootstrapConfig.GetName(),
	}

	return bootstrapRef, nil
}

func (r *TalosControlPlaneReconciler) updateStatus(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	clusterSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			clusterv1.ClusterNameLabel:         cluster.Name,
			clusterv1.MachineControlPlaneLabel: "",
		},
	}

	selector, err := metav1.LabelSelectorAsSelector(clusterSelector)
	if err != nil {
		// Since we are building up the LabelSelector above, this should not fail
		return errors.Wrap(err, "failed to parse label selector")
	}
	// Copy label selector to its status counterpart in string format.
	// This is necessary for CRDs including scale subresources.
	tcp.Status.Selector = selector.String()

	ownedMachines, err := r.getControlPlaneMachinesForCluster(ctx, util.ObjectKey(cluster))
	if err != nil {
		return err
	}

	nonDeletingMachines := collections.FromMachineList(&ownedMachines).Filter(collections.Not(collections.HasDeletionTimestamp))
	replicas := int32(nonDeletingMachines.Len())

	// set basic data that does not require interacting with the workload cluster
	deprecated := tcp.V1Beta1DeprecatedStatus()
	deprecated.Ready = false
	deprecated.UpdatedReplicas = 0
	deprecated.UnavailableReplicas = replicas
	tcp.Status.Replicas = replicas
	tcp.Status.ReadyReplicas = 0
	tcp.Status.AvailableReplicas = ptr.To[int32](0)
	tcp.Status.UpToDateReplicas = ptr.To[int32](0)

	// Return early if the deletion timestamp is set, we don't want to try to connect to the workload cluster.
	if !tcp.DeletionTimestamp.IsZero() {
		return nil
	}

	lowestVersion := nonDeletingMachines.LowestVersion()
	if lowestVersion != "" {
		tcp.Status.Version = &lowestVersion
	}

	controlPlane, err := newControlPlane(ctx, r.Client, cluster, tcp, nonDeletingMachines)
	if err != nil {
		r.Log.Info("failed to compute updated replica status", "error", err)
	} else {
		deprecated.UpdatedReplicas = int32(len(controlPlane.Machines) - len(controlPlane.MachinesWithOutdatedRolloutSpec()))
	}

	// Count replica states from owned Machine conditions. This is the source of truth and
	// is independent of workload-cluster reachability, so AvailableReplicas / UpToDateReplicas /
	// ReadyReplicas remain consistent with Replicas even when the workload API is unreachable.
	var availableReplicas, upToDateReplicas, readyReplicas int32
	for i := range ownedMachines.Items {
		machine := &ownedMachines.Items[i]
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}
		if conditions.IsTrue(machine, clusterv1.MachineAvailableCondition) {
			availableReplicas++
		}
		if conditions.IsTrue(machine, clusterv1.MachineUpToDateCondition) {
			upToDateReplicas++
		}
		if conditions.IsTrue(machine, clusterv1.MachineReadyCondition) {
			readyReplicas++
		}
	}

	tcp.Status.AvailableReplicas = ptr.To(availableReplicas)
	tcp.Status.UpToDateReplicas = ptr.To(upToDateReplicas)
	tcp.Status.ReadyReplicas = readyReplicas

	deprecated.UnavailableReplicas = replicas - readyReplicas
	if readyReplicas > 0 {
		deprecated.Ready = true
	}

	// Probe workload cluster reachability to set Initialized + AvailableCondition. Failure here
	// only suppresses the AvailableCondition; replica counters stay accurate.
	c, err := r.ClusterCache.GetClient(ctx, util.ObjectKey(cluster))
	if err != nil {
		r.Log.Info("failed to get kubeconfig for the cluster", "error", err)

		return nil
	}

	nodeSelector := labels.NewSelector()
	req, err := labels.NewRequirement(constants.LabelNodeRoleControlPlane, selection.Exists, []string{})
	if err != nil {
		return err
	}

	if err := c.List(ctx, &corev1.NodeList{}, &client.ListOptions{LabelSelector: nodeSelector.Add(*req)}); err != nil {
		r.Log.Info("failed to list controlplane nodes", "error", err)

		return nil
	}

	// if we were able to fetch some resources via control plane endpoint,
	// workload cluster control plane endpoint is available
	deprecated.Initialized = true
	tcp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	conditions.Set(tcp, metav1.Condition{
		Type:   string(controlplanev1.AvailableCondition),
		Status: metav1.ConditionTrue,
		Reason: controlplanev1.AvailableReason,
	})

	r.Log.Info("ready replicas", "count", tcp.Status.ReadyReplicas)

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileExternalReference(ctx context.Context, ref clusterv1.ContractVersionedObjectReference, cluster *clusterv1.Cluster) error {
	if !strings.HasSuffix(ref.Kind, clusterv1.TemplateSuffix) {
		return nil
	}

	obj, err := external.GetObjectFromContractVersionedRef(ctx, r.Client, ref, cluster.Namespace)
	if err != nil {
		return err
	}

	objPatchHelper, err := patch.NewHelper(obj, r.Client)
	if err != nil {
		return err
	}

	obj.SetOwnerReferences(util.EnsureOwnerRef(obj.GetOwnerReferences(), metav1.OwnerReference{
		APIVersion: clusterv1.GroupVersion.String(),
		Kind:       "Cluster",
		Name:       cluster.Name,
		UID:        cluster.UID,
	}))

	return objPatchHelper.Patch(ctx, obj)
}

func (r *TalosControlPlaneReconciler) reconcileKubeconfig(ctx context.Context, cp *ControlPlane) (ctrl.Result, error) {
	endpoint := cp.Cluster.Spec.ControlPlaneEndpoint
	if endpoint.IsZero() {
		return ctrl.Result{}, nil
	}

	clusterName := util.ObjectKey(cp.Cluster)
	existingKubeconfig, err := secret.GetFromNamespacedName(ctx, r.Client, clusterName, secret.Kubeconfig)

	switch {
	case apierrors.IsNotFound(err):
		createErr := kubeconfig.CreateSecretWithOwner(
			ctx,
			r.Client,
			clusterName,
			endpoint.String(),
			*metav1.NewControllerRef(cp.TCP, controlplanev1.GroupVersion.WithKind("TalosControlPlane")),
		)
		if createErr != nil {
			if errors.Is(createErr, kubeconfig.ErrDependentCertificateNotFound) {
				r.Log.Info("could not find secret", "secret", secret.ClusterCA, "cluster", clusterName.Name, "namespace", clusterName.Namespace)

				return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
			}

			return ctrl.Result{}, createErr
		}
	case err != nil:
		return ctrl.Result{RequeueAfter: 20 * time.Second}, fmt.Errorf("failed to retrieve kubeconfig Secret for Cluster %q in namespace %q: %w", clusterName.Name, clusterName.Namespace, err)
	default:
		// kubeconfig is already generated
		needsRotation, err := kubeconfig.NeedsClientCertRotation(existingKubeconfig, certs.ClientCertificateRenewalDuration)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to figure out if we need to regenerate cluster client cert: %w", err)
		}

		if !needsRotation {
			return ctrl.Result{}, nil
		}

		r.Log.Info("kubeconfig certificate rotation", "secret", secret.Kubeconfig, "cluster", clusterName.Name, "namespace", clusterName.Namespace)

		err = kubeconfig.RegenerateSecret(ctx, r.Client, existingKubeconfig)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to regenerate kubeconfig: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileEtcdMembers(ctx context.Context, cp *ControlPlane) (result ctrl.Result, err error) {
	var errs error
	// Audit the etcd member list to remove any nodes that no longer exist
	if err := r.auditEtcd(ctx, cp.TCP, util.ObjectKey(cp.Cluster)); err != nil {
		errs = kerrors.NewAggregate([]error{errs, err})
	}

	if err := r.etcdHealthcheck(ctx, cp.TCP, cp.Machines.SortedByCreationTimestamp()); err != nil {
		conditions.Set(cp.TCP, metav1.Condition{
			Type:    string(controlplanev1.EtcdClusterHealthyCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.EtcdClusterUnhealthyReason,
			Message: fmt.Sprintf("Failed to perform etcd healthcheck: %v", err),
		})
		errs = kerrors.NewAggregate([]error{errs, err})
	} else {
		conditions.Set(cp.TCP, metav1.Condition{
			Type:   string(controlplanev1.EtcdClusterHealthyCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.EtcdClusterHealthyReason,
		})
	}

	if errs != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, errs
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileNodeHealth(ctx context.Context, cp *ControlPlane) (result ctrl.Result, err error) {
	if err := r.nodesHealthcheck(ctx, cp.TCP, cp.Machines.SortedByCreationTimestamp()); err != nil {
		reason := controlplanev1.ControlPlaneComponentsInspectionFailedReason

		if errors.Is(err, &errServiceUnhealthy{}) {
			reason = controlplanev1.ControlPlaneComponentsUnhealthyReason
		}

		conditions.Set(cp.TCP, metav1.Condition{
			Type:    string(controlplanev1.ControlPlaneComponentsHealthyCondition),
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: fmt.Sprintf("Failed to perform control plane healthcheck: %v", err),
		})

		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	} else {
		conditions.Set(cp.TCP, metav1.Condition{
			Type:   string(controlplanev1.ControlPlaneComponentsHealthyCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.ControlPlaneComponentsHealthyReason,
		})
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileConditions(ctx context.Context, cp *ControlPlane) (result ctrl.Result, err error) {
	if !conditions.Has(cp.TCP, string(controlplanev1.AvailableCondition)) {
		conditions.Set(cp.TCP, metav1.Condition{
			Type:    string(controlplanev1.AvailableCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.WaitingForTalosBootReason,
			Message: "Waiting for Talos to bootstrap",
		})
	}

	if !conditions.Has(cp.TCP, string(controlplanev1.MachinesBootstrapped)) {
		conditions.Set(cp.TCP, metav1.Condition{
			Type:    string(controlplanev1.MachinesBootstrapped),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.WaitingForMachinesReason,
			Message: "Waiting for machines to bootstrap",
		})
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileMachineTemplateState(ctx context.Context, cp *ControlPlane) error {
	desiredLabels := controlPlaneMachineLabelsForCluster(cp.TCP, cp.Cluster.Name)
	desiredAnnotations := copyStringMap(cp.TCP.Spec.MachineTemplate.ObjectMeta.Annotations)
	desiredReadinessGates := copyMachineReadinessGates(cp.TCP.Spec.MachineTemplate.Spec.ReadinessGates)
	desiredDeletionSpec := buildMachineDeletionSpec(cp.TCP.Spec.MachineTemplate.Spec.Deletion)

	for _, machine := range cp.Machines {
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}

		patchHelper, err := patch.NewHelper(machine, r.Client)
		if err != nil {
			return err
		}

		changed := false

		if machine.Labels == nil {
			machine.Labels = map[string]string{}
		}

		for key, value := range desiredLabels {
			if machine.Labels[key] != value {
				machine.Labels[key] = value
				changed = true
			}
		}

		if len(desiredAnnotations) > 0 {
			if machine.Annotations == nil {
				machine.Annotations = map[string]string{}
			}

			for key, value := range desiredAnnotations {
				if machine.Annotations[key] != value {
					machine.Annotations[key] = value
					changed = true
				}
			}
		}

		if !reflect.DeepEqual(machine.Spec.ReadinessGates, desiredReadinessGates) {
			machine.Spec.ReadinessGates = copyMachineReadinessGates(desiredReadinessGates)
			changed = true
		}

		if !reflect.DeepEqual(machine.Spec.Deletion, desiredDeletionSpec) {
			machine.Spec.Deletion = desiredDeletionSpec
			changed = true
		}

		if !changed {
			continue
		}

		if err := patchHelper.Patch(ctx, machine); err != nil {
			return err
		}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileMachineConditions(ctx context.Context, cp *ControlPlane) (ctrl.Result, error) {
	logger := r.Log.WithValues("namespace", cp.TCP.Namespace, "talosControlPlane", cp.TCP.Name)
	outdated := cp.MachinesWithOutdatedRolloutSpec()

	var errs []error
	for _, m := range cp.Machines {
		if !m.DeletionTimestamp.IsZero() {
			continue
		}

		patchHelper, err := patch.NewHelper(m, r.Client)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// Update MachineUpToDateCondition.
		if _, isOutdated := outdated[m.Name]; isOutdated {
			conditions.Set(m, metav1.Condition{
				Type:               string(clusterv1.MachineUpToDateCondition),
				Status:             metav1.ConditionFalse,
				Reason:             clusterv1.MachineNotUpToDateReason,
				ObservedGeneration: m.Generation,
			})
		} else {
			conditions.Set(m, metav1.Condition{
				Type:               string(clusterv1.MachineUpToDateCondition),
				Status:             metav1.ConditionTrue,
				Reason:             clusterv1.MachineUpToDateReason,
				ObservedGeneration: m.Generation,
			})
		}

		if err := patchHelper.Patch(ctx, m, patch.WithOwnedConditions{Conditions: []string{
			string(clusterv1.MachineUpToDateCondition),
		}}); err != nil {
			errs = append(errs, err)
		}
	}

	// Compute MachinesAllReady by aggregating the Ready condition from each owned Machine.
	// We exclude machines undergoing deletion to match CAPI v1.12.x standards.
	nonDeletingMachines := cp.Machines.Filter(collections.Not(collections.HasDeletionTimestamp))
	getters := make([]conditions.Getter, 0, len(nonDeletingMachines))
	for _, m := range nonDeletingMachines {
		getters = append(getters, m)
	}

	if len(getters) > 0 {
		if err := conditions.SetAggregateCondition(
			getters,
			cp.TCP,
			string(clusterv1.ReadyCondition),
			conditions.TargetConditionType(controlplanev1.MachinesAllReadyCondition),
		); err != nil {
			logger.Error(err, "failed to set MachinesAllReady condition")
			errs = append(errs, err)
		}
	} else {
		conditions.Set(cp.TCP, metav1.Condition{
			Type:               string(controlplanev1.MachinesAllReadyCondition),
			Status:             metav1.ConditionFalse,
			Reason:             controlplanev1.WaitingForMachinesReason,
			ObservedGeneration: cp.TCP.Generation,
		})
	}

	return ctrl.Result{}, kerrors.NewAggregate(errs)
}

func (r *TalosControlPlaneReconciler) reconcileMachines(ctx context.Context, cp *ControlPlane) (res ctrl.Result, err error) {
	logger := r.Log.WithValues("namespace", cp.TCP.Namespace, "talosControlPlane", cp.TCP.Name)

	// If we've made it this far, we can assume that all ownedMachines are up to date
	numMachines := len(cp.Machines)
	desiredReplicas := int(cp.TCP.Spec.GetReplicas())

	if err := r.reconcileMachineTemplateState(ctx, cp); err != nil {
		return ctrl.Result{}, err
	}

	needRollout := cp.MachinesNeedingRollout()
	if len(needRollout) > 0 {
		logger.Info("rolling out control plane machines", "needRollout", needRollout.Names())
		conditions.Set(cp.TCP, metav1.Condition{
			Type:    string(controlplanev1.MachinesSpecUpToDateCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.RollingUpdateInProgressReason,
			Message: fmt.Sprintf("Rolling %d replicas with outdated spec (%d replicas up to date)", len(needRollout), len(cp.Machines)-len(needRollout)),
		})

		return r.upgradeControlPlane(ctx, cp.Cluster, cp.TCP, cp, needRollout)
	} else {
		if conditions.Has(cp.TCP, string(controlplanev1.MachinesSpecUpToDateCondition)) {
			conditions.Set(cp.TCP, metav1.Condition{
				Type:   string(controlplanev1.MachinesSpecUpToDateCondition),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesSpecUpToDateReason,
			})
		}
	}

	switch {
	// We are creating the first replica
	case numMachines < desiredReplicas && numMachines == 0:
		// Create new Machine w/ init
		logger.Info("initializing control plane", "Desired", desiredReplicas, "Existing", numMachines)

		return r.bootControlPlane(ctx, cp.Cluster, cp.TCP, cp, true)
	// We are scaling up
	case numMachines < desiredReplicas && numMachines > 0:
		return r.scaleUpControlPlane(ctx, cp.Cluster, cp.TCP, cp)
	// We are scaling down
	case numMachines > desiredReplicas:
		res, err = r.scaleDownControlPlane(ctx, cp.Cluster, cp.TCP, cp, collections.Machines{})
		if err != nil {
			if res.Requeue || res.RequeueAfter > 0 {
				logger.Info("failed to scale down control plane", "error", err)

				return res, nil
			}
		}

		return res, err
	default:
		if !reflect.ValueOf(cp.TCP.Spec.ControlPlaneConfig.InitConfig).IsZero() {
			cp.TCP.Status.Bootstrapped = true
			conditions.Set(cp.TCP, metav1.Condition{
				Type:   string(controlplanev1.MachinesBootstrapped),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesBootstrappedReason,
			})
		}

		if !cp.TCP.Status.Bootstrapped {
			if err := r.bootstrapCluster(ctx, cp.TCP, cp.Machines.SortedByCreationTimestamp()); err != nil {
				conditions.Set(cp.TCP, metav1.Condition{
					Type:    string(controlplanev1.MachinesBootstrapped),
					Status:  metav1.ConditionFalse,
					Reason:  controlplanev1.WaitingForTalosBootReason,
					Message: fmt.Sprintf("Failed to bootstrap cluster: %v", err),
				})

				logger.Info("bootstrap failed, retrying in 20 seconds", "error", err)

				return ctrl.Result{RequeueAfter: time.Second * 20}, nil
			}

			conditions.Set(cp.TCP, metav1.Condition{
				Type:   string(controlplanev1.MachinesBootstrapped),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesBootstrappedReason,
			})

			cp.TCP.Status.Bootstrapped = true
		}

		if conditions.Has(cp.TCP, string(controlplanev1.MachinesAllReadyCondition)) {
			conditions.Set(cp.TCP, metav1.Condition{
				Type:   string(controlplanev1.ResizedCondition),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.ResizedReason,
			})
		}

		conditions.Set(cp.TCP, metav1.Condition{
			Type:   string(controlplanev1.MachinesCreatedCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.MachinesCreatedReason,
		})
	}

	return ctrl.Result{}, nil
}

func controlPlaneMachineLabelsForCluster(tcp *controlplanev1.TalosControlPlane, clusterName string) map[string]string {
	labels := copyStringMap(tcp.Spec.MachineTemplate.ObjectMeta.Labels)
	if labels == nil {
		labels = map[string]string{}
	}

	labels[clusterv1.ClusterNameLabel] = clusterName
	labels[clusterv1.MachineControlPlaneLabel] = ""
	labels[clusterv1.MachineControlPlaneNameLabel] = mustFormatValue(tcp.Name)

	return labels
}

func mustFormatValue(str string) string {
	if len(validation.IsValidLabelValue(str)) == 0 {
		return str
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(str))

	return fmt.Sprintf("%x", hasher.Sum32())
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}

func copyMachineReadinessGates(in []clusterv1.MachineReadinessGate) []clusterv1.MachineReadinessGate {
	if len(in) == 0 {
		return nil
	}

	out := make([]clusterv1.MachineReadinessGate, len(in))
	copy(out, in)

	return out
}

func buildMachineDeletionSpec(deletion controlplanev1.TalosControlPlaneMachineTemplateDeletionSpec) clusterv1.MachineDeletionSpec {
	return clusterv1.MachineDeletionSpec{
		NodeDrainTimeoutSeconds:        copyInt32Pointer(deletion.NodeDrainTimeoutSeconds),
		NodeVolumeDetachTimeoutSeconds: copyInt32Pointer(deletion.NodeVolumeDetachTimeoutSeconds),
		NodeDeletionTimeoutSeconds:     copyInt32Pointer(deletion.NodeDeletionTimeoutSeconds),
	}
}

func copyInt32Pointer(in *int32) *int32 {
	if in == nil {
		return nil
	}

	out := *in
	return &out
}

func patchTalosControlPlane(ctx context.Context, patchHelper *patch.Helper, tcp *controlplanev1.TalosControlPlane, opts ...patch.Option) error {
	// Always update the readyCondition by summarizing the state of other conditions.
	if err := conditions.SetSummaryCondition(
		tcp,
		tcp,
		clusterv1.ReadyCondition,
		conditions.ForConditionTypes{
			string(controlplanev1.MachinesCreatedCondition),
			string(controlplanev1.ResizedCondition),
			string(controlplanev1.MachinesAllReadyCondition),
			string(controlplanev1.AvailableCondition),
			string(controlplanev1.MachinesBootstrapped),
		},
	); err != nil {
		return errors.Wrap(err, "failed to set summary Ready condition")
	}

	opts = append(opts,
		patch.WithOwnedConditions{Conditions: []string{
			string(controlplanev1.MachinesCreatedCondition),
			string(clusterv1.ReadyCondition),
			string(controlplanev1.ResizedCondition),
			string(controlplanev1.MachinesAllReadyCondition),
			string(controlplanev1.AvailableCondition),
			string(controlplanev1.MachinesBootstrapped),
		}},
	)

	// Patch the object, ignoring conflicts on the conditions owned by this controller.
	return patchHelper.Patch(
		ctx,
		tcp,
		opts...,
	)
}
