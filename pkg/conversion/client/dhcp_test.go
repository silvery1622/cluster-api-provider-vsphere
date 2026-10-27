/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/gomega"
	vmoprv1alpha2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmoprv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	vmoprv1alpha6 "github.com/vmware-tanzu/vm-operator/api/v1alpha6"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	vmoprvhub "sigs.k8s.io/cluster-api-provider-vsphere/pkg/conversion/api/vmoperator/hub"
)

func Test_conversionClient_DHCP(t *testing.T) {
	versions := []struct {
		name      string
		legacy    bool
		newObject func() client.Object
	}{
		{name: "v1alpha2", legacy: true, newObject: func() client.Object { return &vmoprv1alpha2.VirtualMachine{} }},
		{name: "v1alpha5", legacy: true, newObject: func() client.Object { return &vmoprv1alpha5.VirtualMachine{} }},
		{name: "v1alpha6", newObject: func() client.Object { return &vmoprv1alpha6.VirtualMachine{} }},
	}
	states := []struct {
		name       string
		value      *bool
		wantLegacy *bool
	}{
		{name: "unset"},
		// Legacy false has no explicit-disable meaning and converts back to nil.
		{name: "disabled", value: ptr.To(false)},
		{name: "enabled", value: ptr.To(true), wantLegacy: ptr.To(true)},
	}

	for _, version := range versions {
		for _, dhcp4 := range states {
			for _, dhcp6 := range states {
				t.Run(version.name+"/dhcp4="+dhcp4.name+"/dhcp6="+dhcp6.name, func(t *testing.T) {
					g := NewWithT(t)
					ctx := t.Context()
					internalClient := fake.NewClientBuilder().WithScheme(scheme).Build()
					c, err := NewWithConverter(internalClient, converterForVersion(version.name))
					g.Expect(err).NotTo(HaveOccurred())
					vm := &vmoprvhub.VirtualMachine{
						ObjectMeta: metav1.ObjectMeta{Name: "test-vm", Namespace: "test-ns"},
						Spec: vmoprvhub.VirtualMachineSpec{
							Network: &vmoprvhub.VirtualMachineNetworkSpec{
								Interfaces: []vmoprvhub.VirtualMachineNetworkInterfaceSpec{
									{Name: "eth0", DHCP4: dhcp4.value, DHCP6: dhcp6.value},
								},
							},
						},
					}
					want4, want6 := dhcp4.value, dhcp6.value
					if version.legacy {
						want4, want6 = dhcp4.wantLegacy, dhcp6.wantLegacy
					}
					key := client.ObjectKeyFromObject(vm)
					checkDHCP := func(wantMTU *int64) {
						t.Helper()
						got := &vmoprvhub.VirtualMachine{}
						g.Expect(c.Get(ctx, key, got)).To(Succeed())
						g.Expect(got.Spec.Network.Interfaces).To(HaveLen(1))
						g.Expect(got.Spec.Network.Interfaces[0].MTU).To(Equal(wantMTU))
						g.Expect(got.Spec.Network.Interfaces[0].DHCP4).To(Equal(want4))
						g.Expect(got.Spec.Network.Interfaces[0].DHCP6).To(Equal(want6))

						// Check the actual spoke JSON: nil must be omitted, while an
						// explicit v1alpha6 false must remain present on the wire.
						spoke := version.newObject()
						g.Expect(internalClient.Get(ctx, key, spoke)).To(Succeed())
						data, err := json.Marshal(spoke)
						g.Expect(err).NotTo(HaveOccurred())
						var object map[string]interface{}
						g.Expect(json.Unmarshal(data, &object)).To(Succeed())
						interfaces, found, err := unstructured.NestedSlice(object, "spec", "network", "interfaces")
						g.Expect(err).NotTo(HaveOccurred())
						g.Expect(found).To(BeTrue())
						g.Expect(interfaces).To(HaveLen(1))
						for name, want := range map[string]*bool{"dhcp4": want4, "dhcp6": want6} {
							if want == nil {
								g.Expect(interfaces[0]).NotTo(HaveKey(name))
							} else {
								g.Expect(interfaces[0]).To(HaveKeyWithValue(name, *want))
							}
						}
					}

					g.Expect(c.Create(ctx, vm)).To(Succeed())
					g.Expect(vm.Spec.Network.Interfaces[0].DHCP4).To(Equal(want4))
					g.Expect(vm.Spec.Network.Interfaces[0].DHCP6).To(Equal(want6))
					checkDHCP(nil)

					// Changing MTU replaces the interface array in a JSON merge
					// patch, so DHCP intent must survive the conversion as well.
					patch, err := MergeFrom(ctx, c, vm.DeepCopy())
					g.Expect(err).NotTo(HaveOccurred())
					vm.Spec.Network.Interfaces[0].MTU = ptr.To(int64(1450))
					g.Expect(c.Patch(ctx, vm, patch)).To(Succeed())
					checkDHCP(ptr.To(int64(1450)))
				})
			}
		}
	}
}
