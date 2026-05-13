// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers_test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-control-plane-provider-talos/controllers"
)

func TestNextFailureDomainForScaleUp(t *testing.T) {
	fd1 := "fd1"
	fd2 := "fd2"

	failureDomains := []clusterv1.FailureDomain{
		{Name: fd1, ControlPlane: ptr.To(true)},
		{Name: fd2, ControlPlane: ptr.To(true)},
	}

	now := metav1.Now()

	tests := []struct {
		name           string
		cluster        *clusterv1.Cluster
		tcp            *controlplanev1.TalosControlPlane
		machines       collections.Machines
		expectedResult string
	}{
		{
			name: "no failure domains",
			cluster: &clusterv1.Cluster{
				Status: clusterv1.ClusterStatus{},
			},
			tcp:            &controlplanev1.TalosControlPlane{},
			machines:       collections.New(),
			expectedResult: "",
		},
		{
			name: "two failure domains, one has fewer machines",
			cluster: &clusterv1.Cluster{
				Status: clusterv1.ClusterStatus{
					FailureDomains: failureDomains,
				},
			},
			tcp: &controlplanev1.TalosControlPlane{
				Spec: controlplanev1.TalosControlPlaneSpec{
					RolloutStrategy: &controlplanev1.RolloutStrategy{
						Type: controlplanev1.OnDeleteStrategyType,
					},
				},
			},
			machines: collections.FromMachines(
				&clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "m1"}, Spec: clusterv1.MachineSpec{FailureDomain: fd1}},
			),
			expectedResult: fd2,
		},
		{
			name: "two failure domains, tie in machines, picks one (deterministic)",
			cluster: &clusterv1.Cluster{
				Status: clusterv1.ClusterStatus{
					FailureDomains: failureDomains,
				},
			},
			tcp: &controlplanev1.TalosControlPlane{
				Spec: controlplanev1.TalosControlPlaneSpec{
					RolloutStrategy: &controlplanev1.RolloutStrategy{
						Type: controlplanev1.OnDeleteStrategyType,
					},
				},
			},
			machines: collections.New(),
			// Our deterministic tie-breaker sorts failure domains by name if counts are equal.
			// "fd1" < "fd2", so fd1 is picked.
			expectedResult: fd1,
		},
		{
			name: "two failure domains, one has more machines but fewer up-to-date machines",
			cluster: &clusterv1.Cluster{
				Status: clusterv1.ClusterStatus{
					FailureDomains: failureDomains,
				},
			},
			tcp: &controlplanev1.TalosControlPlane{
				Spec: controlplanev1.TalosControlPlaneSpec{
					Version: "v1.30.0",
				},
			},
			machines: collections.FromMachines(
				// fd1 has 1 up-to-date machine.
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{Name: "m1"},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd1,
						Version:       "v1.30.0",
					},
				},
				// fd2 has 2 machines, but both are outdated.
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{Name: "m2"},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd2,
						Version:       "v1.29.0",
					},
				},
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{Name: "m3"},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd2,
						Version:       "v1.29.0",
					},
				},
			),
			// fd2 has 0 up-to-date machines, fd1 has 1. So fd2 is picked.
			expectedResult: fd2,
		},
		{
			name: "two failure domains, tie in up-to-date machines, picks one with fewer machines overall",
			cluster: &clusterv1.Cluster{
				Status: clusterv1.ClusterStatus{
					FailureDomains: failureDomains,
				},
			},
			tcp: &controlplanev1.TalosControlPlane{
				Spec: controlplanev1.TalosControlPlaneSpec{
					Version: "v1.30.0",
				},
			},
			machines: collections.FromMachines(
				// fd1 has 1 up-to-date machine and 1 outdated machine (Total: 2).
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{Name: "m1"},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd1,
						Version:       "v1.30.0",
					},
				},
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{Name: "m2"},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd1,
						Version:       "v1.29.0",
					},
				},
				// fd2 has 1 up-to-date machine (Total: 1).
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{Name: "m3"},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd2,
						Version:       "v1.30.0",
					},
				},
			),
			// Both have 1 up-to-date machine. fd2 has 1 total machine, fd1 has 2. fd2 is picked.
			expectedResult: fd2,
		},
		{
			name: "two failure domains, one has fewer up-to-date machines but they are being deleted",
			cluster: &clusterv1.Cluster{
				Status: clusterv1.ClusterStatus{
					FailureDomains: failureDomains,
				},
			},
			tcp: &controlplanev1.TalosControlPlane{
				Spec: controlplanev1.TalosControlPlaneSpec{
					Version: "v1.30.0",
				},
			},
			machines: collections.FromMachines(
				// fd1 has 1 up-to-date machine.
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{Name: "m1"},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd1,
						Version:       "v1.30.0",
					},
				},
				// fd2 has 1 up-to-date machine, but it has a deletion timestamp.
				&clusterv1.Machine{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "m2",
						DeletionTimestamp: &now,
					},
					Spec: clusterv1.MachineSpec{
						FailureDomain: fd2,
						Version:       "v1.30.0",
					},
				},
			),
			// fd2 has 0 up-to-date non-deleted machines, fd1 has 1. fd2 is picked.
			expectedResult: fd2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			cp := &controllers.ControlPlane{
				Cluster:  tt.cluster,
				TCP:      tt.tcp,
				Machines: tt.machines,
			}
			result, err := cp.NextFailureDomainForScaleUp(context.Background())
			g.Expect(err).ToNot(HaveOccurred())
			g.Expect(result).To(Equal(tt.expectedResult))
		})
	}
}

func TestNextFailureDomainForScaleUpLargeVolume(t *testing.T) {
	g := NewWithT(t)

	// Create 20 failure domains: fd01, fd02, ..., fd20
	var fds []clusterv1.FailureDomain
	for i := 1; i <= 20; i++ {
		name := fmt.Sprintf("fd%02d", i)
		fds = append(fds, clusterv1.FailureDomain{
			Name:         name,
			ControlPlane: ptr.To(true),
		})
	}

	cluster := &clusterv1.Cluster{
		Status: clusterv1.ClusterStatus{
			FailureDomains: fds,
		},
	}
	tcp := &controlplanev1.TalosControlPlane{
		Spec: controlplanev1.TalosControlPlaneSpec{
			RolloutStrategy: &controlplanev1.RolloutStrategy{
				Type: controlplanev1.OnDeleteStrategyType,
			},
		},
	}

	// Case 1: All domains are empty. Should pick fd01 (alphabetically first).
	cp := &controllers.ControlPlane{
		Cluster:  cluster,
		TCP:      tcp,
		Machines: collections.New(),
	}
	result, err := cp.NextFailureDomainForScaleUp(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(result).To(Equal("fd01"))

	// Case 2: Some domains have machines. Should pick the one with fewest.
	// We'll put machines in fd01, fd02, ..., fd19.
	var machines []*clusterv1.Machine
	for i := 1; i <= 19; i++ {
		machines = append(machines, &clusterv1.Machine{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("m%02d", i)},
			Spec:       clusterv1.MachineSpec{FailureDomain: fmt.Sprintf("fd%02d", i)},
		})
	}
	cp.Machines = collections.FromMachines(machines...)

	// Now fd20 is the only one with 0 machines. It should be picked.
	result, err = cp.NextFailureDomainForScaleUp(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(result).To(Equal("fd20"))

	// Case 3: All domains have 1 machine except fd10 and fd15 which have 0.
	// Should pick fd10 (alphabetically first among those with 0).
	machines = nil
	for i := 1; i <= 20; i++ {
		if i == 10 || i == 15 {
			continue
		}
		machines = append(machines, &clusterv1.Machine{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("m%02d", i)},
			Spec:       clusterv1.MachineSpec{FailureDomain: fmt.Sprintf("fd%02d", i)},
		})
	}
	cp.Machines = collections.FromMachines(machines...)

	result, err = cp.NextFailureDomainForScaleUp(context.Background())
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(result).To(Equal("fd10"))
}
