// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"reflect"
	"sort"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Log is the global logger for the internal package.
var Log = klog.Background()

// ControlPlane holds business logic around control planes.
// It should never need to connect to a service, that responsibility lies outside of this struct.
type ControlPlane struct {
	TCP      *controlplanev1.TalosControlPlane
	Cluster  *clusterv1.Cluster
	Machines collections.Machines

	infraObjects map[string]*unstructured.Unstructured
	talosConfigs map[string]*cabptv1.TalosConfig
}

// newControlPlane returns an instantiated ControlPlane.
func newControlPlane(ctx context.Context, client client.Client, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines collections.Machines) (*ControlPlane, error) {
	infraObjects, err := getInfraResources(ctx, client, machines, cluster.Namespace)
	if err != nil {
		return nil, err
	}

	talosConfigs, err := getTalosConfigs(ctx, client, machines)
	if err != nil {
		return nil, err
	}

	return &ControlPlane{
		TCP:          tcp,
		Cluster:      cluster,
		Machines:     machines,
		infraObjects: infraObjects,
		talosConfigs: talosConfigs,
	}, nil
}

// Logger returns a logger with useful context.
func (c *ControlPlane) Logger() logr.Logger {
	return Log.WithValues("namespace", c.TCP.Namespace, "name", c.TCP.Name, "cluster-name", c.Cluster.Name)
}

// MachineWithDeleteAnnotation returns a machine that has been annotated with DeleteMachineAnnotation key.
func (c *ControlPlane) MachineWithDeleteAnnotation(machines collections.Machines) collections.Machines {
	// See if there are any machines with DeleteMachineAnnotation key.
	annotatedMachines := machines.Filter(collections.HasAnnotationKey(clusterv1.DeleteMachineAnnotation))
	// If there are, return list of annotated machines.
	return annotatedMachines
}

// FailureDomains returns a slice of failure domain objects synced from the infrastructure provider into Cluster.Status
// that are flagged for control plane use.
func (c *ControlPlane) FailureDomains() []clusterv1.FailureDomain {
	if c.Cluster.Status.FailureDomains == nil {
		return nil
	}

	var res []clusterv1.FailureDomain
	for _, fd := range c.Cluster.Status.FailureDomains {
		if ptr.Deref(fd.ControlPlane, false) {
			res = append(res, fd)
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Name < res[j].Name
	})

	return res
}

// FailureDomainWithMostMachines returns the failure domain with most machines in it and at least one eligible machine in it.
// Note: if there are eligible machines in a failure domain that no longer exists, getting rid of those machines takes precedence.
func (c *ControlPlane) FailureDomainWithMostMachines(ctx context.Context, eligibleMachines collections.Machines) string {
	// See if there are any Machines that are not in currently defined failure domains first.
	notInFailureDomains := eligibleMachines.Filter(
		collections.Not(collections.InFailureDomains(c.getFailureDomainIDs()...)),
	)
	if len(notInFailureDomains) > 0 {
		// Return the failure domain for the oldest Machine not in the current list of failure domains.
		// This could be either empty (no failure domain defined) or a failure domain that is no longer defined
		// in the cluster status.
		return notInFailureDomains.Oldest().Spec.FailureDomain
	}

	// Pick the failure domain with most machines in it and at least one eligible machine in it.
	return c.pickMost(ctx, c.FailureDomains(), c.Machines, eligibleMachines)
}

// MachineInFailureDomainWithMostMachines returns the first matching failure domain with machines that has the most
// control-plane machines on it.
func (c *ControlPlane) MachineInFailureDomainWithMostMachines(ctx context.Context, eligibleMachines collections.Machines) (*clusterv1.Machine, error) {
	fd := c.FailureDomainWithMostMachines(ctx, eligibleMachines)
	machinesInFailureDomain := eligibleMachines.Filter(collections.InFailureDomains(fd))
	machineToMark := machinesInFailureDomain.Oldest()
	if machineToMark == nil {
		return nil, errors.New("failed to pick control plane Machine to mark for deletion")
	}
	return machineToMark, nil
}

// NextFailureDomainForScaleUp returns the failure domain with the fewest number of up-to-date, not deleted machines.
// In case of tie, the failure domain with the fewest number of machines overall is picked.
func (c *ControlPlane) NextFailureDomainForScaleUp(ctx context.Context) (string, error) {
	if len(c.FailureDomains()) == 0 {
		return "", nil
	}
	upToDateMachines := c.Machines.Difference(c.MachinesNeedingRollout())
	return c.pickFewest(ctx, c.FailureDomains(), c.Machines, upToDateMachines.Filter(collections.Not(collections.HasDeletionTimestamp))), nil
}

type failureDomainAggregation struct {
	id            string
	countPriority int
	countAll      int
}

// pickFewest returns the failure domain with the fewest number of up-to-date, not deleted machines.
// In case of tie, the failure domain with the fewest number of machines overall is picked.
// If there is still a tie, the failure domain with the alphabetically smaller name is picked.
func (c *ControlPlane) pickFewest(ctx context.Context, failureDomains []clusterv1.FailureDomain, allMachines, upToDateMachines collections.Machines) string {
	aggregations := c.countByFailureDomain(ctx, failureDomains, allMachines, upToDateMachines)
	if len(aggregations) == 0 {
		return ""
	}

	sort.SliceStable(aggregations, func(i, j int) bool {
		if aggregations[i].countPriority != aggregations[j].countPriority {
			return aggregations[i].countPriority < aggregations[j].countPriority
		}
		if aggregations[i].countAll != aggregations[j].countAll {
			return aggregations[i].countAll < aggregations[j].countAll
		}
		return aggregations[i].id < aggregations[j].id
	})

	return aggregations[0].id
}

// pickMost returns the failure domain from which we have to delete a control plane machine,
// which is the failure domain with most machines and at least one eligible machine in it.
// If there is a tie, the failure domain with the alphabetically smaller name is picked.
func (c *ControlPlane) pickMost(ctx context.Context, failureDomains []clusterv1.FailureDomain, allMachines, eligibleMachines collections.Machines) string {
	aggregations := c.countByFailureDomain(ctx, failureDomains, allMachines, eligibleMachines)
	if len(aggregations) == 0 {
		return ""
	}

	sort.SliceStable(aggregations, func(i, j int) bool {
		if aggregations[i].countPriority != aggregations[j].countPriority {
			return aggregations[i].countPriority > aggregations[j].countPriority
		}
		if aggregations[i].countAll != aggregations[j].countAll {
			return aggregations[i].countAll > aggregations[j].countAll
		}
		return aggregations[i].id < aggregations[j].id
	})

	if aggregations[0].countPriority > 0 {
		return aggregations[0].id
	}

	return ""
}

func (c *ControlPlane) countByFailureDomain(_ context.Context, failureDomains []clusterv1.FailureDomain, allMachines, priorityMachines collections.Machines) []failureDomainAggregation {
	if len(failureDomains) == 0 {
		return nil
	}

	counters := make(map[string]failureDomainAggregation, len(failureDomains))
	for _, fd := range failureDomains {
		counters[fd.Name] = failureDomainAggregation{
			id: fd.Name,
		}
	}

	for _, m := range allMachines {
		if m.Spec.FailureDomain == "" {
			continue
		}
		if agg, ok := counters[m.Spec.FailureDomain]; ok {
			agg.countAll++
			counters[m.Spec.FailureDomain] = agg
		}
	}

	for _, m := range priorityMachines {
		if m.Spec.FailureDomain == "" {
			continue
		}
		if agg, ok := counters[m.Spec.FailureDomain]; ok {
			agg.countPriority++
			counters[m.Spec.FailureDomain] = agg
		}
	}

	res := make([]failureDomainAggregation, 0, len(counters))
	for _, agg := range counters {
		res = append(res, agg)
	}

	return res
}

// getFailureDomainIDs returns the IDs of the failure domains.
func (c *ControlPlane) getFailureDomainIDs() []string {
	fds := c.FailureDomains()
	ids := make([]string, 0, len(fds))
	for _, fd := range fds {
		ids = append(ids, fd.Name)
	}
	return ids
}

// MachinesNeedingRollout return a list of machines that need to be rolled out.
func (c *ControlPlane) MachinesNeedingRollout() collections.Machines {
	if c.TCP.Spec.RolloutStrategy != nil && c.TCP.Spec.RolloutStrategy.Type == controlplanev1.OnDeleteStrategyType {
		return collections.New()
	}

	return c.MachinesWithOutdatedRolloutSpec()
}

// MachinesWithOutdatedRolloutSpec returns Machines whose rollout-requiring fields do not match the desired control plane spec.
func (c *ControlPlane) MachinesWithOutdatedRolloutSpec() collections.Machines {
	// Ignore machines to be deleted.
	machines := c.Machines.Filter(collections.Not(collections.HasDeletionTimestamp))

	// Return machines if they are scheduled for rollout or if with an outdated configuration.
	return machines.AnyFilter(
		// Machines that do not match with TCP config.
		collections.Not(
			collections.And(
				collections.MatchesKubernetesVersion(c.TCP.Spec.Version),
				MatchesTemplateClonedFrom(c.infraObjects, c.TCP),
				MatchesControlPlaneConfig(c.talosConfigs, c.TCP),
			),
		),
	)
}

// getInfraResources fetches the external infrastructure resource for each machine in the collection and returns a map of machine.Name -> infraResource.
func getInfraResources(ctx context.Context, cl client.Client, machines collections.Machines, namespace string) (map[string]*unstructured.Unstructured, error) {
	result := map[string]*unstructured.Unstructured{}
	for _, m := range machines {
		infraObj, err := external.GetObjectFromContractVersionedRef(ctx, cl, m.Spec.InfrastructureRef, namespace)
		if err != nil {
			if apierrors.IsNotFound(errors.Cause(err)) {
				continue
			}
			return nil, errors.Wrapf(err, "failed to retrieve infra obj for machine %q", m.Name)
		}
		result[m.Name] = infraObj
	}
	return result, nil
}

// getTalosConfigs fetches the TalosConfigs for each machine in the collection and returns a map of machine.Name -> TalosConfig.
func getTalosConfigs(ctx context.Context, cl client.Client, machines collections.Machines) (map[string]*cabptv1.TalosConfig, error) {
	result := map[string]*cabptv1.TalosConfig{}

	for _, m := range machines {
		bootstrapRef := m.Spec.Bootstrap.ConfigRef
		if !bootstrapRef.IsDefined() {
			continue
		}

		talosconfig := &cabptv1.TalosConfig{}

		err := cl.Get(ctx, client.ObjectKey{
			Namespace: m.Namespace,
			Name:      bootstrapRef.Name,
		}, talosconfig)
		if err != nil {
			if apierrors.IsNotFound(errors.Cause(err)) {
				continue
			}
			return nil, errors.Wrapf(err, "failed to retrieve talosconfig obj for machine %q", m.Name)
		}

		result[m.Name] = talosconfig
	}

	return result, nil
}

// MatchesTemplateClonedFrom returns a filter to find all machines that match a given TCP infra template.
func MatchesTemplateClonedFrom(infraConfigs map[string]*unstructured.Unstructured, tcp *controlplanev1.TalosControlPlane) collections.Func {
	return func(machine *clusterv1.Machine) bool {
		if machine == nil {
			return false
		}
		infraObj, found := infraConfigs[machine.Name]
		if !found {
			// Return true here because failing to get infrastructure machine should not be considered as unmatching.
			return true
		}

		clonedFromName, ok1 := infraObj.GetAnnotations()[clusterv1.TemplateClonedFromNameAnnotation]
		clonedFromGroupKind, ok2 := infraObj.GetAnnotations()[clusterv1.TemplateClonedFromGroupKindAnnotation]
		if !ok1 || !ok2 {
			// All tcp cloned infra machines should have this annotation.
			// Missing the annotation may be due to older version machines or adopted machines.
			// Should not be considered as mismatch.
			return true
		}

		templateRef := tcp.Spec.MachineTemplate.Spec.InfrastructureRef

		// Check if the machine's infrastructure reference has been created from the current TCP infrastructure template.
		templateGroupKind := schema.GroupKind{Group: templateRef.APIGroup, Kind: templateRef.Kind}.String()
		if clonedFromName != templateRef.Name || clonedFromGroupKind != templateGroupKind {
			return false
		}

		return true
	}
}

// MatchesControlPlaneConfig returns a filter to find all machines that match a given controlPaneConfig.
func MatchesControlPlaneConfig(talosConfigs map[string]*cabptv1.TalosConfig, tcp *controlplanev1.TalosControlPlane) collections.Func {
	return func(machine *clusterv1.Machine) bool {
		if machine == nil {
			return false
		}

		talosConfig, found := talosConfigs[machine.Name]
		if !found {
			// Return true here because failing to get talosconfig should not be considered as unmatching.
			return true
		}

		return reflect.DeepEqual(tcp.Spec.ControlPlaneConfig.ControlPlaneConfig, talosConfig.Spec)
	}
}
