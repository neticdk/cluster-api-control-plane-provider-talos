// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	ctrl "sigs.k8s.io/controller-runtime"
)

// ReconcileMachineConditions is a test-only export of the private reconcileMachineConditions method.
func (r *TalosControlPlaneReconciler) ReconcileMachineConditions(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (ctrl.Result, error) {
	cp, err := newControlPlane(ctx, r.Client, cluster, tcp, collections.FromMachineList(machines))
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.reconcileMachineConditions(ctx, cp)
}

// TalosconfigForMachines is a test-only export of the private talosconfigForMachines method.
func (r *TalosControlPlaneReconciler) TalosconfigForMachines(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines ...clusterv1.Machine) (*talosclient.Client, error) {
	return r.talosconfigForMachines(ctx, tcp, machines...)
}

// GenerateTalosConfig is a test-only export of the private generateTalosConfig method.
func (r *TalosControlPlaneReconciler) GenerateTalosConfig(ctx context.Context, tcp *controlplanev1.TalosControlPlane, name string, spec *cabptv1.TalosConfigSpec) (*clusterv1.ContractVersionedObjectReference, error) {
	return r.generateTalosConfig(ctx, tcp, name, spec)
}
