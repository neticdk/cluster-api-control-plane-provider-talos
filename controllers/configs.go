// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	talosconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// talosconfigForMachine will generate a talosconfig that uses *all* found addresses as the endpoints.
//
// NOTE: There is no client.WithNodes(...) here, so no multiplexing is done. The request will hit any
// of the controlplane nodes in machines list.
func (r *TalosControlPlaneReconciler) talosconfigForMachines(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines ...clusterv1.Machine) (*talosclient.Client, error) {
	if len(machines) == 0 {
		return nil, fmt.Errorf("at least one machine should be provided")
	}

	clusterName := tcp.GetLabels()[clusterv1.ClusterNameLabel]

	for _, ref := range tcp.GetOwnerReferences() {
		if ref.Kind != "Cluster" {
			continue
		}

		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if gv.Group == clusterv1.GroupVersion.Group {
			clusterName = ref.Name

			break
		}
	}

	if clusterName == "" {
		return nil, fmt.Errorf("failed to determine the cluster name of the control plane")
	}

	if !reflect.ValueOf(tcp.Spec.ControlPlaneConfig.InitConfig).IsZero() {
		return r.talosconfigFromWorkloadCluster(ctx, tcp, client.ObjectKey{Namespace: tcp.GetNamespace(), Name: clusterName}, machines...)
	}

	addrList := []string{}

	if tcp.Spec.ControlPlaneEndpoint.IsValid() {
		addrList = append(addrList, tcp.Spec.ControlPlaneEndpoint.Host)
	}

	var talosconfigSecret corev1.Secret

	if err := r.Get(ctx,
		types.NamespacedName{
			Namespace: tcp.GetNamespace(),
			Name:      clusterName + "-talosconfig",
		},
		&talosconfigSecret,
	); err != nil {
		return nil, err
	}

	t, err := talosconfig.FromBytes(talosconfigSecret.Data["talosconfig"])
	if err != nil {
		return nil, err
	}

	for _, machine := range machines {
		for _, addr := range machine.Status.Addresses {
			if addr.Type == clusterv1.MachineExternalIP || addr.Type == clusterv1.MachineInternalIP {
				addrList = append(addrList, addr.Address)
			}
		}

		if len(addrList) == 0 {
			return nil, fmt.Errorf("no addresses were found for node %q", machine.Name)
		}
	}

	// we don't need to set endpoints in general here, as endpoints were already pre-populated by the CABPT controller
	// but we use the `machines` to _limit_ access to a specific machine in some places, and we need to be compatible
	// with talosconfigFromWorkloadCluster which doesn't rely on Machine's Addresses
	//
	// once we're done with Sidero and `init` nodes, we can switch to use `WithNodes` and proper Machine IPs
	return talosclient.New(ctx,
		talosclient.WithDefaultGRPCDialOptions(),
		talosclient.WithEndpoints(addrList...),
		talosclient.WithConfig(t),
	)
}

// talosconfigFromWorkloadCluster gets talosconfig and populates endoints using workload cluster nodes.
func (r *TalosControlPlaneReconciler) talosconfigFromWorkloadCluster(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster client.ObjectKey, machines ...clusterv1.Machine) (*talosclient.Client, error) {
	if len(machines) == 0 {
		return nil, fmt.Errorf("at least one machine should be provided")
	}

	c, err := r.ClusterCache.GetClient(ctx, cluster)
	if err != nil {
		return nil, err
	}

	addrList := []string{}

	if tcp.Spec.ControlPlaneEndpoint.IsValid() {
		addrList = append(addrList, tcp.Spec.ControlPlaneEndpoint.Host)
	}

	var t *talosconfig.Config

	for _, machine := range machines {
		if !machine.Status.NodeRef.IsDefined() {
			return nil, fmt.Errorf("%q machine does not have a nodeRef", machine.Name)
		}

		var node corev1.Node

		// grab all addresses as endpoints
		err := c.Get(ctx, types.NamespacedName{Name: machine.Status.NodeRef.Name}, &node)
		if err != nil {
			return nil, err
		}

		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeExternalIP || addr.Type == corev1.NodeInternalIP {
				addrList = append(addrList, addr.Address)
			}
		}

		if len(addrList) == 0 {
			return nil, fmt.Errorf("no addresses were found for node %q", node.Name)
		}

		if t == nil {
			var talosconfigSecret corev1.Secret

			if err := r.Get(ctx,
				types.NamespacedName{
					Namespace: tcp.GetNamespace(),
					Name:      cluster.Name + "-talosconfig",
				},
				&talosconfigSecret,
			); err != nil {
				return nil, err
			}

			data, ok := talosconfigSecret.Data["talosconfig"]
			if !ok {
				return nil, fmt.Errorf("talosconfig secret %s/%s does not contain 'talosconfig' key", talosconfigSecret.Namespace, talosconfigSecret.Name)
			}

			t, err = talosconfig.FromBytes(data)
			if err != nil {
				return nil, err
			}
		}
	}

	return talosclient.New(ctx,
		talosclient.WithDefaultGRPCDialOptions(),
		talosclient.WithEndpoints(addrList...),
		talosclient.WithConfig(t),
	)
}
