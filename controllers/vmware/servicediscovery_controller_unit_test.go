/*
Copyright 2021 The Kubernetes Authors.

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

package vmware

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	vmoprv1alpha6 "github.com/vmware-tanzu/vm-operator/api/v1alpha6"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/component-base/featuregate"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capiutil "sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmwarev1 "sigs.k8s.io/cluster-api-provider-vsphere/api/supervisor/v1beta2"
	"sigs.k8s.io/cluster-api-provider-vsphere/feature"
	vmwarehelpers "sigs.k8s.io/cluster-api-provider-vsphere/internal/test/helpers/vmware"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/context/fake"
	conversionapi "sigs.k8s.io/cluster-api-provider-vsphere/pkg/conversion/api"
	conversionclient "sigs.k8s.io/cluster-api-provider-vsphere/pkg/conversion/client"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/services"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/services/network"
)

type dummyDualStackNetworkProvider struct {
	services.NetworkProvider
}

func (d *dummyDualStackNetworkProvider) SupportsIPv6DualStack() bool {
	return true
}

func (d *dummyDualStackNetworkProvider) HasLoadBalancer() bool {
	return true
}

var _ = Describe("ServiceDiscoveryReconciler reconcileNormal", serviceDiscoveryUnitTestsReconcileNormal)

func serviceDiscoveryUnitTestsReconcileNormal() {
	var (
		controllerCtx  *vmwarehelpers.UnitTestContextForController
		vsphereCluster vmwarev1.VSphereCluster
		initObjects    []client.Object
		reconciler     serviceDiscoveryReconciler
	)
	namespace := capiutil.RandomString(6)
	JustBeforeEach(func() {
		vsphereCluster = fake.NewVSphereCluster(namespace)
		controllerCtx = vmwarehelpers.NewUnitTestContextForController(ctx, namespace, &vsphereCluster, false, initObjects, nil)
		reconciler = serviceDiscoveryReconciler{
			Client:          controllerCtx.ControllerManagerContext.Client,
			NetworkProvider: network.DummyNetworkProvider(),
		}
		err := reconciler.reconcileNormal(ctx, controllerCtx.GuestClusterContext)
		Expect(err).NotTo(HaveOccurred())
	})
	JustAfterEach(func() {
		controllerCtx = nil
	})
	Context("When no VIP or FIP is available ", func() {
		It("Should reconcile headless svc", func() {
			By("creating a service and no endpoint in the guest cluster")
			assertHeadlessSvcWithNoEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionFalse, "Failed to discover supervisor API server endpoint",
				vmwarev1.VSphereClusterServiceDiscoveryNotReadyReason)
		})
	})
	Context("When VIP is available", func() {
		BeforeEach(func() {
			initObjects = []client.Object{ //nolint:prealloc
				newTestSupervisorLBServiceWithIPStatus(),
			}
			initObjects = append(initObjects, newTestHeadlessSvcEndpoints()...)
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and endpoints using the VIP in the guest cluster")
			assertHeadlessSvcWithVIPEndpoints(ctx, controllerCtx.GuestClient, vmwarev1.SupervisorHeadlessSvcNamespace, vmwarev1.SupervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionTrue, "", vmwarev1.VSphereClusterServiceDiscoveryReadyReason)
		})
		It("Should get supervisor master endpoint IP", func() {
			r := &serviceDiscoveryReconciler{
				Client:          controllerCtx.ControllerManagerContext.Client,
				NetworkProvider: network.DummyNetworkProvider(),
			}
			supervisorEndpointIPs, err := r.getSupervisorAPIServerAddress(ctx, controllerCtx.Cluster)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(supervisorEndpointIPs).To(Equal([]string{testSupervisorAPIServerVIP}))
		})
	})
	Context("When FIP is available", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestConfigMapWithHost(testSupervisorAPIServerFIP)}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and endpoints using the FIP in the guest cluster")
			assertHeadlessSvcWithFIPEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionTrue, "", vmwarev1.VSphereClusterServiceDiscoveryReadyReason)
		})
	})
	Context("When VIP and FIP are available", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestSupervisorLBServiceWithIPStatus(),
				newTestConfigMapWithHost(testSupervisorAPIServerFIP),
			}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and endpoints using the VIP in the guest cluster")
			assertHeadlessSvcWithVIPEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionTrue, "", vmwarev1.VSphereClusterServiceDiscoveryReadyReason)
		})
	})
	Context("When VIP is an hostname", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestSupervisorLBServiceWithHostnameStatus()}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and no endpoint in the guest cluster")
			assertHeadlessSvcWithNoEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionFalse, "must be an IP address",
				vmwarev1.VSphereClusterServiceDiscoveryNotReadyReason)
		})
	})
	Context("When FIP is an hostname", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestConfigMapWithHost(testSupervisorAPIServerFIPHostName),
			}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and no endpoint in the guest cluster")
			assertHeadlessSvcWithNoEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionFalse, "must be an IP address",
				vmwarev1.VSphereClusterServiceDiscoveryNotReadyReason)
		})
	})
	Context("When FIP is an empty hostname", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestConfigMapWithHost(""),
			}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and no endpoint in the guest cluster")
			assertHeadlessSvcWithNoEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionFalse, "Failed to discover supervisor API server endpoint",
				vmwarev1.VSphereClusterServiceDiscoveryNotReadyReason)
		})
	})
	Context("When VIP is an invalid host", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestConfigMapWithHost("host^name"),
			}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and no endpoint in the guest cluster")
			assertHeadlessSvcWithNoEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionFalse, "Failed to discover supervisor API server endpoint",
				vmwarev1.VSphereClusterServiceDiscoveryNotReadyReason)
		})
	})
		Context("When DualStack is supported and IPv4/IPv6 VIPs are available", func() {
			BeforeEach(func() {
				initObjects = []client.Object{
					newTestSupervisorLBServiceWithDualStackStatus(),
				}
				err := feature.Gates.(featuregate.MutableFeatureGate).Set(fmt.Sprintf("%s=true", feature.IPv6DualStack))
				Expect(err).ShouldNot(HaveOccurred())
			})
			AfterEach(func() {
				err := feature.Gates.(featuregate.MutableFeatureGate).Set(fmt.Sprintf("%s=false", feature.IPv6DualStack))
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("Should get dual stack supervisor master endpoint IPs", func() {
				controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
					Pods: clusterv1.NetworkRanges{
						CIDRBlocks: []string{"192.168.0.0/16", "fd00:100:96::/48"},
					},
				}
				converter := conversionapi.DefaultConverterFor(vmoprv1alpha6.GroupVersion)
				cc, err := conversionclient.NewWithConverter(controllerCtx.ControllerManagerContext.Client, converter)
				Expect(err).ShouldNot(HaveOccurred())

				r := &serviceDiscoveryReconciler{
				Client:          cc,
				NetworkProvider: &dummyDualStackNetworkProvider{},
			}
			supervisorEndpointIPs, err := r.getSupervisorAPIServerAddress(ctx, controllerCtx.Cluster)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(supervisorEndpointIPs).To(Equal([]string{testSupervisorAPIServerVIP, testSupervisorAPIServerIPv6VIP}))
		})
	})
		Context("When DualStack is supported and only IPv6 VIP is available (IPv6 Single Stack)", func() {
			BeforeEach(func() {
				initObjects = []client.Object{
					newTestSupervisorLBServiceWithIPv6Status(),
				}
				err := feature.Gates.(featuregate.MutableFeatureGate).Set(fmt.Sprintf("%s=true", feature.IPv6DualStack))
				Expect(err).ShouldNot(HaveOccurred())
			})
			AfterEach(func() {
				err := feature.Gates.(featuregate.MutableFeatureGate).Set(fmt.Sprintf("%s=false", feature.IPv6DualStack))
				Expect(err).ShouldNot(HaveOccurred())
			})
			It("Should get IPv6 supervisor master endpoint IP", func() {
				controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
					Pods: clusterv1.NetworkRanges{
						CIDRBlocks: []string{"fd00:100:96::/48"},
					},
				}
				converter := conversionapi.DefaultConverterFor(vmoprv1alpha6.GroupVersion)
				cc, err := conversionclient.NewWithConverter(controllerCtx.ControllerManagerContext.Client, converter)
				Expect(err).ShouldNot(HaveOccurred())

				r := &serviceDiscoveryReconciler{
				Client:          cc,
				NetworkProvider: &dummyDualStackNetworkProvider{},
			}
			supervisorEndpointIPs, err := r.getSupervisorAPIServerAddress(ctx, controllerCtx.Cluster)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(supervisorEndpointIPs).To(Equal([]string{testSupervisorAPIServerIPv6VIP}))
		})
	})
	Context("When FIP config map has invalid kubeconfig data", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestConfigMapWithData(
					map[string]string{
						bootstrapapi.KubeConfigKey: "invalid-kubeconfig-data",
					}),
			}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and no endpoint in the guest cluster")
			assertHeadlessSvcWithNoEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionFalse, "Failed to discover supervisor API server endpoint",
				vmwarev1.VSphereClusterServiceDiscoveryNotReadyReason)
		})
	})
	Context("When FIP config map has invalid kubeconfig key", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestConfigMapWithData(
					map[string]string{
						"invalid-key": "invalid-kubeconfig-data",
					}),
			}
		})
		It("Should reconcile headless svc", func() {
			By("creating a service and no endpoint in the guest cluster")
			assertHeadlessSvcWithNoEndpoints(ctx, controllerCtx.GuestClient, supervisorHeadlessSvcNamespace, supervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionFalse, "Failed to discover supervisor API server endpoint",
				vmwarev1.VSphereClusterServiceDiscoveryNotReadyReason)
		})
	})
}
