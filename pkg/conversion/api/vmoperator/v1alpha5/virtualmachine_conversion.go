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

package v1alpha5

import (
	"context"

	vmoprv1alpha5 "github.com/vmware-tanzu/vm-operator/api/v1alpha5"
	vmoprv1alpha5common "github.com/vmware-tanzu/vm-operator/api/v1alpha5/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	utilconversion "sigs.k8s.io/cluster-api/util/conversion"

	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/conversion"
	vmoprvhub "sigs.k8s.io/cluster-api-provider-vsphere/pkg/conversion/api/vmoperator/hub"
)

func convert_v1alpha5_VirtualMachine_To_hub_VirtualMachine(_ context.Context, src *vmoprv1alpha5.VirtualMachine, dst *vmoprvhub.VirtualMachine) error {
	dst.ObjectMeta = src.ObjectMeta

	if ok, err := utilconversion.UnmarshalData(src, dst); err != nil {
		return err
	} else if ok {
		delete(dst.Annotations, "cluster.x-k8s.io/conversion-data")
		if len(dst.Annotations) == 0 {
			dst.Annotations = nil
		}
	}

	if src.Spec.Affinity != nil {
		if dst.Spec.Affinity == nil {
			dst.Spec.Affinity = &vmoprvhub.AffinitySpec{}
		}
		if src.Spec.Affinity.VMAffinity != nil {
			if dst.Spec.Affinity.VMAffinity == nil {
				dst.Spec.Affinity.VMAffinity = &vmoprvhub.VMAffinitySpec{}
			}
			if src.Spec.Affinity.VMAffinity.RequiredDuringSchedulingPreferredDuringExecution != nil {
				terms := []vmoprvhub.VMAffinityTerm{}
				for _, term := range src.Spec.Affinity.VMAffinity.RequiredDuringSchedulingPreferredDuringExecution {
					t := vmoprvhub.VMAffinityTerm{
						TopologyKey: term.TopologyKey,
					}
					if term.LabelSelector != nil {
						t.LabelSelector = term.LabelSelector.DeepCopy()
					}
					terms = append(terms, t)
				}
				dst.Spec.Affinity.VMAffinity.RequiredDuringSchedulingPreferredDuringExecution = terms
			} else {
				dst.Spec.Affinity.VMAffinity.RequiredDuringSchedulingPreferredDuringExecution = nil
			}
		} else {
			dst.Spec.Affinity.VMAffinity = nil
		}
		if src.Spec.Affinity.VMAntiAffinity != nil {
			if dst.Spec.Affinity.VMAntiAffinity == nil {
				dst.Spec.Affinity.VMAntiAffinity = &vmoprvhub.VMAntiAffinitySpec{}
			}
			if src.Spec.Affinity.VMAntiAffinity.PreferredDuringSchedulingPreferredDuringExecution != nil {
				terms := []vmoprvhub.VMAffinityTerm{}
				for _, term := range src.Spec.Affinity.VMAntiAffinity.PreferredDuringSchedulingPreferredDuringExecution {
					t := vmoprvhub.VMAffinityTerm{
						TopologyKey: term.TopologyKey,
					}
					if term.LabelSelector != nil {
						t.LabelSelector = term.LabelSelector.DeepCopy()
					}
					terms = append(terms, t)
				}
				dst.Spec.Affinity.VMAntiAffinity.PreferredDuringSchedulingPreferredDuringExecution = terms
			} else {
				dst.Spec.Affinity.VMAntiAffinity.PreferredDuringSchedulingPreferredDuringExecution = nil
			}
		} else {
			dst.Spec.Affinity.VMAntiAffinity = nil
		}
	} else {
		dst.Spec.Affinity = nil
	}
	if src.Spec.Bootstrap != nil {
		if dst.Spec.Bootstrap == nil {
			dst.Spec.Bootstrap = &vmoprvhub.VirtualMachineBootstrapSpec{}
		}
		if src.Spec.Bootstrap.CloudInit != nil {
			if dst.Spec.Bootstrap.CloudInit == nil {
				dst.Spec.Bootstrap.CloudInit = &vmoprvhub.VirtualMachineBootstrapCloudInitSpec{}
			}
			if src.Spec.Bootstrap.CloudInit.RawCloudConfig != nil {
				dst.Spec.Bootstrap.CloudInit.RawCloudConfig = &vmoprvhub.SecretKeySelector{
					Name: src.Spec.Bootstrap.CloudInit.RawCloudConfig.Name,
					Key:  src.Spec.Bootstrap.CloudInit.RawCloudConfig.Key,
				}
			} else {
				dst.Spec.Bootstrap.CloudInit.RawCloudConfig = nil
			}
		} else {
			dst.Spec.Bootstrap.CloudInit = nil
		}
	} else {
		dst.Spec.Bootstrap = nil
	}
	dst.Spec.ClassName = src.Spec.ClassName
	dst.Spec.GroupName = src.Spec.GroupName
	dst.Spec.ImageName = src.Spec.ImageName
	if src.Spec.Network != nil {
		if dst.Spec.Network == nil {
			dst.Spec.Network = &vmoprvhub.VirtualMachineNetworkSpec{}
		}
		if src.Spec.Network.Interfaces != nil {
			restoredInterfaces := map[string]vmoprvhub.VirtualMachineNetworkInterfaceSpec{}
			for _, iface := range dst.Spec.Network.Interfaces {
				restoredInterfaces[iface.Name] = iface
			}

			dst.Spec.Network.Interfaces = []vmoprvhub.VirtualMachineNetworkInterfaceSpec{}
			for _, iface := range src.Spec.Network.Interfaces {
				d, ok := restoredInterfaces[iface.Name]
				if !ok {
					d = vmoprvhub.VirtualMachineNetworkInterfaceSpec{}
				}
				d.Addresses = iface.Addresses
				// AdvancedProperties existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				d.DHCP4 = iface.DHCP4
				d.DHCP6 = iface.DHCP6
				d.Gateway4 = iface.Gateway4
				d.Gateway6 = iface.Gateway6
				// IPAMModes existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				if iface.MTU != nil {
					d.MTU = ptr.To(*iface.MTU)
				}
				if iface.Network != nil {
					d.Network = &vmoprvhub.PartialObjectRef{
						TypeMeta: metav1.TypeMeta{
							Kind:       iface.Network.Kind,
							APIVersion: iface.Network.APIVersion,
						},
						Name: iface.Network.Name,
					}
				}
				d.GuestDeviceName = iface.GuestDeviceName
				d.Name = iface.Name
				d.Nameservers = iface.Nameservers
				d.MACAddr = iface.MACAddr
				if iface.Routes != nil {
					d.Routes = []vmoprvhub.VirtualMachineNetworkRouteSpec{}
					for _, route := range iface.Routes {
						d.Routes = append(d.Routes, vmoprvhub.VirtualMachineNetworkRouteSpec{
							To:     route.To,
							Via:    route.Via,
							Metric: route.Metric,
						})
					}
				}
				d.SearchDomains = iface.SearchDomains
				// Type existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				// VMXNet3 existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				// VNUMANodeID existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				dst.Spec.Network.Interfaces = append(dst.Spec.Network.Interfaces, d)
			}
		}
	} else {
		dst.Spec.Network = nil
	}
	dst.Spec.MinHardwareVersion = src.Spec.MinHardwareVersion
	if src.Spec.Policies != nil {
		dst.Spec.Policies = make([]vmoprvhub.PolicySpec, 0, len(src.Spec.Policies))
		for _, p := range src.Spec.Policies {
			dst.Spec.Policies = append(dst.Spec.Policies, vmoprvhub.PolicySpec{
				APIVersion: p.APIVersion,
				Kind:       p.Kind,
				Name:       p.Name,
			})
		}
	}
	dst.Spec.PowerOffMode = vmoprvhub.VirtualMachinePowerOpMode(src.Spec.PowerOffMode)
	dst.Spec.PowerState = vmoprvhub.VirtualMachinePowerState(src.Spec.PowerState)
	if src.Spec.ReadinessProbe != nil {
		if dst.Spec.ReadinessProbe == nil {
			dst.Spec.ReadinessProbe = &vmoprvhub.VirtualMachineReadinessProbeSpec{}
		}
		if src.Spec.ReadinessProbe.TCPSocket != nil {
			dst.Spec.ReadinessProbe.TCPSocket = &vmoprvhub.TCPSocketAction{
				Port: src.Spec.ReadinessProbe.TCPSocket.Port,
				Host: src.Spec.ReadinessProbe.TCPSocket.Host,
			}
		} else {
			dst.Spec.ReadinessProbe.TCPSocket = nil
		}
	} else {
		dst.Spec.ReadinessProbe = nil
	}
	if src.Spec.Reserved != nil {
		if dst.Spec.Reserved == nil {
			dst.Spec.Reserved = &vmoprvhub.VirtualMachineReservedSpec{}
		}
		dst.Spec.Reserved.ResourcePolicyName = src.Spec.Reserved.ResourcePolicyName
	} else {
		dst.Spec.Reserved = nil
	}
	dst.Spec.StorageClass = src.Spec.StorageClass
	if src.Spec.Volumes != nil {
		restoredVolumes := map[string]vmoprvhub.VirtualMachineVolume{}
		for _, volume := range dst.Spec.Volumes {
			restoredVolumes[volume.Name] = volume
		}

		dst.Spec.Volumes = []vmoprvhub.VirtualMachineVolume{}
		for _, volume := range src.Spec.Volumes {
			v, ok := restoredVolumes[volume.Name]
			if !ok {
				v = vmoprvhub.VirtualMachineVolume{}
			}
			v.ApplicationType = vmoprvhub.VolumeApplicationType(volume.ApplicationType)
			if volume.ControllerBusNumber != nil {
				v.ControllerBusNumber = ptr.To(*volume.ControllerBusNumber)
			} else {
				v.ControllerBusNumber = nil
			}
			v.ControllerType = vmoprvhub.VirtualControllerType(volume.ControllerType)
			v.DiskMode = vmoprvhub.VolumeDiskMode(volume.DiskMode)
			v.Name = volume.Name
			if volume.PersistentVolumeClaim != nil {
				if v.PersistentVolumeClaim == nil {
					v.PersistentVolumeClaim = &vmoprvhub.PersistentVolumeClaimVolumeSource{}
				}
				v.PersistentVolumeClaim.PersistentVolumeClaimVolumeSource = corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: volume.PersistentVolumeClaim.ClaimName,
					ReadOnly:  volume.PersistentVolumeClaim.ReadOnly,
				}
				if volume.PersistentVolumeClaim.InstanceVolumeClaim != nil {
					if v.PersistentVolumeClaim.InstanceVolumeClaim == nil {
						v.PersistentVolumeClaim.InstanceVolumeClaim = &vmoprvhub.InstanceVolumeClaimVolumeSource{}
					}
					v.PersistentVolumeClaim.InstanceVolumeClaim.StorageClass = volume.PersistentVolumeClaim.InstanceVolumeClaim.StorageClass
					v.PersistentVolumeClaim.InstanceVolumeClaim.Size = volume.PersistentVolumeClaim.InstanceVolumeClaim.Size
				} else {
					v.PersistentVolumeClaim.InstanceVolumeClaim = nil
				}
			} else {
				v.PersistentVolumeClaim = nil
			}
			v.Removable = volume.Removable
			v.SharingMode = vmoprvhub.VolumeSharingMode(volume.SharingMode)
			if volume.UnitNumber != nil {
				v.UnitNumber = ptr.To(*volume.UnitNumber)
			} else {
				v.UnitNumber = nil
			}
			dst.Spec.Volumes = append(dst.Spec.Volumes, v)
		}
	} else {
		dst.Spec.Volumes = nil
	}

	dst.Status.BiosUUID = src.Status.BiosUUID
	if src.Status.Conditions != nil {
		dst.Status.Conditions = []metav1.Condition{}
		for _, condition := range src.Status.Conditions {
			dst.Status.Conditions = append(dst.Status.Conditions, condition)
		}
	}
	if src.Status.Network != nil {
		if dst.Status.Network == nil {
			dst.Status.Network = &vmoprvhub.VirtualMachineNetworkStatus{}
		}
		dst.Status.Network.HostName = src.Status.Network.HostName
		if src.Status.Network.Interfaces != nil {
			restoredInterfaces := map[int32]vmoprvhub.VirtualMachineNetworkInterfaceStatus{}
			for _, iface := range dst.Status.Network.Interfaces {
				restoredInterfaces[iface.DeviceKey] = iface
			}

			dst.Status.Network.Interfaces = []vmoprvhub.VirtualMachineNetworkInterfaceStatus{}
			for _, iface := range src.Status.Network.Interfaces {
				d, ok := restoredInterfaces[iface.DeviceKey]
				if !ok {
					d = vmoprvhub.VirtualMachineNetworkInterfaceStatus{}
				}
				d.DeviceKey = iface.DeviceKey
				if iface.DNS != nil {
					if d.DNS == nil {
						d.DNS = &vmoprvhub.VirtualMachineNetworkDNSStatus{}
					}
					d.DNS.DHCP = iface.DNS.DHCP
					d.DNS.HostName = iface.DNS.HostName
					d.DNS.DomainName = iface.DNS.DomainName
					d.DNS.Nameservers = iface.DNS.Nameservers
					d.DNS.SearchDomains = iface.DNS.SearchDomains
				} else {
					d.DNS = nil
				}
				if iface.IP != nil {
					if d.IP == nil {
						d.IP = &vmoprvhub.VirtualMachineNetworkInterfaceIPStatus{}
					}
					d.IP.MACAddr = iface.IP.MACAddr
					if iface.IP.Addresses != nil {
						d.IP.Addresses = []vmoprvhub.VirtualMachineNetworkInterfaceIPAddrStatus{}
						for _, addr := range iface.IP.Addresses {
							d.IP.Addresses = append(d.IP.Addresses, vmoprvhub.VirtualMachineNetworkInterfaceIPAddrStatus{
								Address:  addr.Address,
								Lifetime: addr.Lifetime,
								Origin:   addr.Origin,
								State:    addr.State,
							})
						}
					} else {
						d.IP.Addresses = nil
					}
					if iface.IP.AutoConfigurationEnabled != nil {
						d.IP.AutoConfigurationEnabled = ptr.To(*iface.IP.AutoConfigurationEnabled)
					} else {
						d.IP.AutoConfigurationEnabled = nil
					}
					if iface.IP.DHCP != nil {
						if d.IP.DHCP == nil {
							d.IP.DHCP = &vmoprvhub.VirtualMachineNetworkDHCPStatus{}
						}
						d.IP.DHCP.IP4.Enabled = iface.IP.DHCP.IP4.Enabled
						d.IP.DHCP.IP6.Enabled = iface.IP.DHCP.IP6.Enabled

						if iface.IP.DHCP.IP4.Config != nil {
							d.IP.DHCP.IP4.Config = []vmoprvhub.KeyValuePair{}
							for _, pair := range iface.IP.DHCP.IP4.Config {
								d.IP.DHCP.IP4.Config = append(d.IP.DHCP.IP4.Config, vmoprvhub.KeyValuePair{
									Key:   pair.Key,
									Value: pair.Value,
								})
							}
						} else {
							d.IP.DHCP.IP4.Config = nil
						}
						if iface.IP.DHCP.IP6.Config != nil {
							d.IP.DHCP.IP6.Config = []vmoprvhub.KeyValuePair{}
							for _, pair := range iface.IP.DHCP.IP6.Config {
								d.IP.DHCP.IP6.Config = append(d.IP.DHCP.IP6.Config, vmoprvhub.KeyValuePair{
									Key:   pair.Key,
									Value: pair.Value,
								})
							}
						} else {
							d.IP.DHCP.IP6.Config = nil
						}
					} else {
						d.IP.DHCP = nil
					}
				} else {
					d.IP = nil
				}
				d.Name = iface.Name
				dst.Status.Network.Interfaces = append(dst.Status.Network.Interfaces, d)
			}
		} else {
			dst.Status.Network.Interfaces = nil
		}
		dst.Status.Network.PrimaryIP4 = src.Status.Network.PrimaryIP4
		dst.Status.Network.PrimaryIP6 = src.Status.Network.PrimaryIP6
	} else {
		dst.Status.Network = nil
	}
	dst.Status.NodeName = src.Status.NodeName
	if src.Status.Policies != nil {
		dst.Status.Policies = make([]vmoprvhub.PolicyStatus, 0, len(src.Status.Policies))
		for _, p := range src.Status.Policies {
			dst.Status.Policies = append(dst.Status.Policies, vmoprvhub.PolicyStatus{
				PolicySpec: vmoprvhub.PolicySpec{
					APIVersion: p.APIVersion,
					Kind:       p.Kind,
					Name:       p.Name,
				},
				Generation: p.Generation,
			})
		}
	}
	dst.Status.PowerState = vmoprvhub.VirtualMachinePowerState(src.Status.PowerState)
	dst.Status.Zone = src.Status.Zone

	return nil
}

func convert_hub_VirtualMachine_To_v1alpha5_VirtualMachine(_ context.Context, src *vmoprvhub.VirtualMachine, dst *vmoprv1alpha5.VirtualMachine) error {
	dst.ObjectMeta = src.ObjectMeta

	if err := utilconversion.MarshalData(src, dst); err != nil {
		return err
	}

	if src.Spec.Affinity != nil {
		dst.Spec.Affinity = &vmoprv1alpha5.AffinitySpec{}
		if src.Spec.Affinity.VMAffinity != nil {
			dst.Spec.Affinity.VMAffinity = &vmoprv1alpha5.VMAffinitySpec{}
			if src.Spec.Affinity.VMAffinity.RequiredDuringSchedulingPreferredDuringExecution != nil {
				terms := []vmoprv1alpha5.VMAffinityTerm{}
				for _, term := range src.Spec.Affinity.VMAffinity.RequiredDuringSchedulingPreferredDuringExecution {
					t := vmoprv1alpha5.VMAffinityTerm{
						TopologyKey: term.TopologyKey,
					}
					if term.LabelSelector != nil {
						t.LabelSelector = term.LabelSelector.DeepCopy()
					}
					terms = append(terms, t)
				}
				dst.Spec.Affinity.VMAffinity.RequiredDuringSchedulingPreferredDuringExecution = terms
			}
		}
		if src.Spec.Affinity.VMAntiAffinity != nil {
			dst.Spec.Affinity.VMAntiAffinity = &vmoprv1alpha5.VMAntiAffinitySpec{}
			if src.Spec.Affinity.VMAntiAffinity.PreferredDuringSchedulingPreferredDuringExecution != nil {
				terms := []vmoprv1alpha5.VMAffinityTerm{}
				for _, term := range src.Spec.Affinity.VMAntiAffinity.PreferredDuringSchedulingPreferredDuringExecution {
					t := vmoprv1alpha5.VMAffinityTerm{
						TopologyKey: term.TopologyKey,
					}
					if term.LabelSelector != nil {
						t.LabelSelector = term.LabelSelector.DeepCopy()
					}
					terms = append(terms, t)
				}
				dst.Spec.Affinity.VMAntiAffinity.PreferredDuringSchedulingPreferredDuringExecution = terms
			}
		}
	}
	if src.Spec.Bootstrap != nil {
		dst.Spec.Bootstrap = &vmoprv1alpha5.VirtualMachineBootstrapSpec{}
		if src.Spec.Bootstrap.CloudInit != nil {
			dst.Spec.Bootstrap.CloudInit = &vmoprv1alpha5.VirtualMachineBootstrapCloudInitSpec{}
			if src.Spec.Bootstrap.CloudInit.RawCloudConfig != nil {
				dst.Spec.Bootstrap.CloudInit.RawCloudConfig = &vmoprv1alpha5common.SecretKeySelector{
					Name: src.Spec.Bootstrap.CloudInit.RawCloudConfig.Name,
					Key:  src.Spec.Bootstrap.CloudInit.RawCloudConfig.Key,
				}
			}
		}
	}
	dst.Spec.ClassName = src.Spec.ClassName
	dst.Spec.GroupName = src.Spec.GroupName
	dst.Spec.ImageName = src.Spec.ImageName
	if src.Spec.Network != nil {
		dst.Spec.Network = &vmoprv1alpha5.VirtualMachineNetworkSpec{}
		if src.Spec.Network.Interfaces != nil {
			dst.Spec.Network.Interfaces = []vmoprv1alpha5.VirtualMachineNetworkInterfaceSpec{}
			for _, iface := range src.Spec.Network.Interfaces {
				d := vmoprv1alpha5.VirtualMachineNetworkInterfaceSpec{}
				d.Addresses = iface.Addresses
				// AdvancedProperties existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				d.DHCP4 = iface.DHCP4
				d.DHCP6 = iface.DHCP6
				d.Gateway4 = iface.Gateway4
				d.Gateway6 = iface.Gateway6
				// IPAMModes existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				if iface.MTU != nil {
					d.MTU = ptr.To(*iface.MTU)
				}
				if iface.Network != nil {
					d.Network = &vmoprv1alpha5common.PartialObjectRef{
						TypeMeta: metav1.TypeMeta{
							Kind:       iface.Network.Kind,
							APIVersion: iface.Network.APIVersion,
						},
						Name: iface.Network.Name,
					}
				}
				d.GuestDeviceName = iface.GuestDeviceName
				d.Name = iface.Name
				d.Nameservers = iface.Nameservers
				d.MACAddr = iface.MACAddr
				if iface.Routes != nil {
					d.Routes = []vmoprv1alpha5.VirtualMachineNetworkRouteSpec{}
					for _, route := range iface.Routes {
						d.Routes = append(d.Routes, vmoprv1alpha5.VirtualMachineNetworkRouteSpec{
							To:     route.To,
							Via:    route.Via,
							Metric: route.Metric,
						})
					}
				}
				d.SearchDomains = iface.SearchDomains
				// Type existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				// VMXNet3 existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				// VNUMANodeID existing in hub but not in v1alpha5.VirtualMachineNetworkInterfaceSpec
				dst.Spec.Network.Interfaces = append(dst.Spec.Network.Interfaces, d)
			}
		}
	}
	dst.Spec.MinHardwareVersion = src.Spec.MinHardwareVersion
	if src.Spec.Policies != nil {
		dst.Spec.Policies = make([]vmoprv1alpha5.PolicySpec, 0, len(src.Spec.Policies))
		for _, p := range src.Spec.Policies {
			dst.Spec.Policies = append(dst.Spec.Policies, vmoprv1alpha5.PolicySpec{
				APIVersion: p.APIVersion,
				Kind:       p.Kind,
				Name:       p.Name,
			})
		}
	}
	dst.Spec.PowerOffMode = vmoprv1alpha5.VirtualMachinePowerOpMode(src.Spec.PowerOffMode)
	dst.Spec.PowerState = vmoprv1alpha5.VirtualMachinePowerState(src.Spec.PowerState)
	if src.Spec.ReadinessProbe != nil {
		dst.Spec.ReadinessProbe = &vmoprv1alpha5.VirtualMachineReadinessProbeSpec{}
		if src.Spec.ReadinessProbe.TCPSocket != nil {
			dst.Spec.ReadinessProbe.TCPSocket = &vmoprv1alpha5.TCPSocketAction{
				Port: src.Spec.ReadinessProbe.TCPSocket.Port,
				Host: src.Spec.ReadinessProbe.TCPSocket.Host,
			}
		}
	}
	if src.Spec.Reserved != nil {
		dst.Spec.Reserved = &vmoprv1alpha5.VirtualMachineReservedSpec{
			ResourcePolicyName: src.Spec.Reserved.ResourcePolicyName,
		}
	}
	dst.Spec.StorageClass = src.Spec.StorageClass
	if src.Spec.Volumes != nil {
		dst.Spec.Volumes = []vmoprv1alpha5.VirtualMachineVolume{}
		for _, volume := range src.Spec.Volumes {
			v := vmoprv1alpha5.VirtualMachineVolume{}
			v.ApplicationType = vmoprv1alpha5.VolumeApplicationType(volume.ApplicationType)
			if volume.ControllerBusNumber != nil {
				v.ControllerBusNumber = ptr.To(*volume.ControllerBusNumber)
			}
			v.ControllerType = vmoprv1alpha5.VirtualControllerType(volume.ControllerType)
			v.DiskMode = vmoprv1alpha5.VolumeDiskMode(volume.DiskMode)
			v.Name = volume.Name
			if volume.PersistentVolumeClaim != nil {
				v.PersistentVolumeClaim = &vmoprv1alpha5.PersistentVolumeClaimVolumeSource{
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: volume.PersistentVolumeClaim.ClaimName,
						ReadOnly:  volume.PersistentVolumeClaim.ReadOnly,
					},
				}
				if volume.PersistentVolumeClaim.InstanceVolumeClaim != nil {
					v.PersistentVolumeClaim.InstanceVolumeClaim = &vmoprv1alpha5.InstanceVolumeClaimVolumeSource{
						StorageClass: volume.PersistentVolumeClaim.InstanceVolumeClaim.StorageClass,
						Size:         volume.PersistentVolumeClaim.InstanceVolumeClaim.Size,
					}
				}

			}
			v.Removable = volume.Removable
			v.SharingMode = vmoprv1alpha5.VolumeSharingMode(volume.SharingMode)
			if volume.UnitNumber != nil {
				v.UnitNumber = ptr.To(*volume.UnitNumber)
			}
			dst.Spec.Volumes = append(dst.Spec.Volumes, v)
		}
	}

	dst.Status.BiosUUID = src.Status.BiosUUID
	if src.Status.Conditions != nil {
		dst.Status.Conditions = []metav1.Condition{}
		for _, condition := range src.Status.Conditions {
			dst.Status.Conditions = append(dst.Status.Conditions, condition)
		}
	}
	dst.Status.NodeName = src.Status.NodeName
	if src.Status.Network != nil {
		dst.Status.Network = &vmoprv1alpha5.VirtualMachineNetworkStatus{}
		dst.Status.Network.HostName = src.Status.Network.HostName
		if src.Status.Network.Interfaces != nil {
			dst.Status.Network.Interfaces = []vmoprv1alpha5.VirtualMachineNetworkInterfaceStatus{}
			for _, iface := range src.Status.Network.Interfaces {
				d := vmoprv1alpha5.VirtualMachineNetworkInterfaceStatus{}
				d.DeviceKey = iface.DeviceKey
				if iface.DNS != nil {
					d.DNS = &vmoprv1alpha5.VirtualMachineNetworkDNSStatus{
						DHCP:          iface.DNS.DHCP,
						HostName:      iface.DNS.HostName,
						DomainName:    iface.DNS.DomainName,
						Nameservers:   iface.DNS.Nameservers,
						SearchDomains: iface.DNS.SearchDomains,
					}
				}
				if iface.IP != nil {
					d.IP = &vmoprv1alpha5.VirtualMachineNetworkInterfaceIPStatus{
						MACAddr: iface.IP.MACAddr,
					}
					if iface.IP.Addresses != nil {
						d.IP.Addresses = []vmoprv1alpha5.VirtualMachineNetworkInterfaceIPAddrStatus{}
						for _, addr := range iface.IP.Addresses {
							d.IP.Addresses = append(d.IP.Addresses, vmoprv1alpha5.VirtualMachineNetworkInterfaceIPAddrStatus{
								Address:  addr.Address,
								Lifetime: addr.Lifetime,
								Origin:   addr.Origin,
								State:    addr.State,
							})
						}
					}
					if iface.IP.AutoConfigurationEnabled != nil {
						d.IP.AutoConfigurationEnabled = ptr.To(*iface.IP.AutoConfigurationEnabled)
					}
					if iface.IP.DHCP != nil {
						d.IP.DHCP = &vmoprv1alpha5.VirtualMachineNetworkDHCPStatus{
							IP4: vmoprv1alpha5.VirtualMachineNetworkDHCPOptionsStatus{
								Enabled: iface.IP.DHCP.IP4.Enabled,
							},
							IP6: vmoprv1alpha5.VirtualMachineNetworkDHCPOptionsStatus{
								Enabled: iface.IP.DHCP.IP6.Enabled,
							},
						}
						if iface.IP.DHCP.IP4.Config != nil {
							d.IP.DHCP.IP4.Config = []vmoprv1alpha5common.KeyValuePair{}
							for _, pair := range iface.IP.DHCP.IP4.Config {
								d.IP.DHCP.IP4.Config = append(d.IP.DHCP.IP4.Config, vmoprv1alpha5common.KeyValuePair{
									Key:   pair.Key,
									Value: pair.Value,
								})
							}
						}
						if iface.IP.DHCP.IP6.Config != nil {
							d.IP.DHCP.IP6.Config = []vmoprv1alpha5common.KeyValuePair{}
							for _, pair := range iface.IP.DHCP.IP6.Config {
								d.IP.DHCP.IP6.Config = append(d.IP.DHCP.IP6.Config, vmoprv1alpha5common.KeyValuePair{
									Key:   pair.Key,
									Value: pair.Value,
								})
							}
						}
					}
				}
				d.Name = iface.Name
				dst.Status.Network.Interfaces = append(dst.Status.Network.Interfaces, d)
			}
		}
		dst.Status.Network.PrimaryIP4 = src.Status.Network.PrimaryIP4
		dst.Status.Network.PrimaryIP6 = src.Status.Network.PrimaryIP6
	}
	if src.Status.Policies != nil {
		dst.Status.Policies = make([]vmoprv1alpha5.PolicyStatus, 0, len(src.Status.Policies))
		for _, p := range src.Status.Policies {
			dst.Status.Policies = append(dst.Status.Policies, vmoprv1alpha5.PolicyStatus{
				PolicySpec: vmoprv1alpha5.PolicySpec{
					APIVersion: p.APIVersion,
					Kind:       p.Kind,
					Name:       p.Name,
				},
				Generation: p.Generation,
			})
		}
	}
	dst.Status.PowerState = vmoprv1alpha5.VirtualMachinePowerState(src.Status.PowerState)
	dst.Status.Zone = src.Status.Zone

	return nil
}

func init() {
	converterBuilder.AddConversion(
		conversion.NewAddConversionBuilder(convert_hub_VirtualMachine_To_v1alpha5_VirtualMachine, convert_v1alpha5_VirtualMachine_To_hub_VirtualMachine),
	)
}
