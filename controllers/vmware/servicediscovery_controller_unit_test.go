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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	capiutil "sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/controller-runtime/pkg/client"

	vmwarev1 "sigs.k8s.io/cluster-api-provider-vsphere/api/supervisor/v1beta2"
	vmwarehelpers "sigs.k8s.io/cluster-api-provider-vsphere/internal/test/helpers/vmware"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/context/fake"
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
		netProvider    services.NetworkProvider
	)
	namespace := capiutil.RandomString(6)
	BeforeEach(func() {
		netProvider = network.DummyNetworkProvider()
	})
	JustBeforeEach(func() {
		vsphereCluster = fake.NewVSphereCluster(namespace)
		controllerCtx = vmwarehelpers.NewUnitTestContextForController(ctx, namespace, &vsphereCluster, false, initObjects, nil)
		reconciler = serviceDiscoveryReconciler{
			Client:          controllerCtx.ControllerManagerContext.Client,
			NetworkProvider: netProvider,
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
			supervisorEndpointIP, err := r.getSupervisorAPIServerAddress(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(supervisorEndpointIP).To(Equal(testSupervisorAPIServerVIP))
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
		})
		It("Should get dual stack supervisor master endpoint IPs", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{
					CIDRBlocks: []string{"192.168.0.0/16", "fd00:100:96::/48"},
				},
			}

			r := &serviceDiscoveryReconciler{
				Client:          controllerCtx.ControllerManagerContext.Client,
				NetworkProvider: &dummyDualStackNetworkProvider{},
			}
			supervisorEndpointIPs, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(supervisorEndpointIPs).To(Equal([]string{testSupervisorAPIServerVIP, testSupervisorAPIServerIPv6VIP}))
		})
	})
	Context("When DualStack is supported and only IPv6 VIP is available (IPv6 Single Stack)", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestSupervisorLBServiceWithIPv6Status(),
			}
		})
		It("Should get IPv6 supervisor master endpoint IP", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{
					CIDRBlocks: []string{"fd00:100:96::/48"},
				},
			}

			r := &serviceDiscoveryReconciler{
				Client:          controllerCtx.ControllerManagerContext.Client,
				NetworkProvider: &dummyDualStackNetworkProvider{},
			}
			supervisorEndpointIPs, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(supervisorEndpointIPs).To(Equal([]string{testSupervisorAPIServerIPv6VIP}))
		})
	})
	Context("When DualStack is supported and VIP contains a hostname", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestSupervisorLBServiceWithHostnameStatus(),
			}
		})
		It("Should not panic and return error if no valid IP is found", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{
					CIDRBlocks: []string{"192.168.0.0/16", "fd00:100:96::/48"},
				},
			}

			r := &serviceDiscoveryReconciler{
				Client:          controllerCtx.ControllerManagerContext.Client,
				NetworkProvider: &dummyDualStackNetworkProvider{},
			}
			_, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no supervisor apiserver VIP found for dual stack cluster"))
		})
	})
	Context("getSupervisorAPIServerAddresses permutations", func() {
		var r serviceDiscoveryReconciler
		BeforeEach(func() {
			r = serviceDiscoveryReconciler{
				Client:          controllerCtx.ControllerManagerContext.Client,
				NetworkProvider: &dummyDualStackNetworkProvider{},
			}
		})

		It("should return error for IPv6 single stack cluster when only IPv4 VIP is available", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{CIDRBlocks: []string{"fd00:1::/64"}},
			}
			initObjects = []client.Object{newTestSupervisorLBServiceWithIPStatus()}
			controllerCtx = vmwarehelpers.NewUnitTestContextForController(ctx, namespace, &vsphereCluster, false, initObjects, nil)
			r.Client = controllerCtx.ControllerManagerContext.Client

			_, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no supervisor apiserver IPv6 VIP found for IPv6 single stack cluster"))
		})

		It("should return error when more than 2 VIPs are found", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{CIDRBlocks: []string{"192.168.0.0/16", "fd00:1::/64"}},
			}
			svc := newTestSupervisorLBService()
			svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
				{IP: "10.0.0.1"},
				{IP: "10.0.0.2"},
				{IP: "fd00::1"},
			}
			initObjects = []client.Object{svc}
			controllerCtx = vmwarehelpers.NewUnitTestContextForController(ctx, namespace, &vsphereCluster, false, initObjects, nil)
			r.Client = controllerCtx.ControllerManagerContext.Client

			_, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("found too many VIPs"))
		})

		It("should return error when an invalid IP is found in VIPs", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{CIDRBlocks: []string{"192.168.0.0/16", "fd00:1::/64"}},
			}

			svc := newTestSupervisorLBService()
			svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
				{IP: "invalid-ip"},
			}
			initObjects = []client.Object{svc}
			controllerCtx = vmwarehelpers.NewUnitTestContextForController(ctx, namespace, &vsphereCluster, false, initObjects, nil)
			r.Client = controllerCtx.ControllerManagerContext.Client

			_, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must be an IP address"))
		})

		It("should return IPv4 only for dual stack cluster when only IPv4 VIP is available", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{CIDRBlocks: []string{"192.168.0.0/16", "fd00:1::/64"}},
			}
			initObjects = []client.Object{newTestSupervisorLBServiceWithIPStatus()}
			controllerCtx = vmwarehelpers.NewUnitTestContextForController(ctx, namespace, &vsphereCluster, false, initObjects, nil)
			r.Client = controllerCtx.ControllerManagerContext.Client

			vips, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(vips).To(Equal([]string{testSupervisorAPIServerVIP}))
		})

		It("should return IPv6 only for dual stack cluster when only IPv6 VIP is available", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{CIDRBlocks: []string{"192.168.0.0/16", "fd00:1::/64"}},
			}
			initObjects = []client.Object{newTestSupervisorLBServiceWithIPv6Status()}
			controllerCtx = vmwarehelpers.NewUnitTestContextForController(ctx, namespace, &vsphereCluster, false, initObjects, nil)
			r.Client = controllerCtx.ControllerManagerContext.Client

			vips, err := r.getSupervisorAPIServerAddresses(ctx, controllerCtx.Cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(vips).To(Equal([]string{testSupervisorAPIServerIPv6VIP}))
		})
	})
	Context("When DualStack is supported and IPv4/IPv6 VIPs are available (reconcileNormal)", func() {
		BeforeEach(func() {
			initObjects = []client.Object{
				newTestSupervisorLBServiceWithDualStackStatus(),
			}
			netProvider = &dummyDualStackNetworkProvider{}
		})
		It("Should reconcile headless svc with dual stack endpoints", func() {
			controllerCtx.Cluster.Spec.ClusterNetwork = clusterv1.ClusterNetwork{
				Pods: clusterv1.NetworkRanges{
					CIDRBlocks: []string{"192.168.0.0/16", "fd00:100:96::/48"},
				},
			}

			// We need to re-run reconcileNormal because JustBeforeEach already ran with the default cluster network
			err := reconciler.reconcileNormal(ctx, controllerCtx.GuestClusterContext)
			Expect(err).NotTo(HaveOccurred())

			By("creating a service and endpoints using both VIPs in the guest cluster")
			assertHeadlessSvcWithDualStackEndpoints(ctx, controllerCtx.GuestClient, vmwarev1.SupervisorHeadlessSvcNamespace, vmwarev1.SupervisorHeadlessSvcName)
			assertServiceDiscoveryCondition(controllerCtx.VSphereCluster, metav1.ConditionTrue, "", vmwarev1.VSphereClusterServiceDiscoveryReadyReason)
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
