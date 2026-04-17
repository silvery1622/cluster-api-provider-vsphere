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

package vmoperator

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	controlplanev1 "sigs.k8s.io/cluster-api/api/controlplane/kubeadm/v1beta2"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	deprecatedv1beta1conditions "sigs.k8s.io/cluster-api/util/conditions/deprecated/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	vmwarev1 "sigs.k8s.io/cluster-api-provider-vsphere/api/supervisor/v1beta2"
	"sigs.k8s.io/cluster-api-provider-vsphere/feature"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/context/vmware"
	vmoprvhub "sigs.k8s.io/cluster-api-provider-vsphere/pkg/conversion/api/vmoperator/hub"
	conversionclient "sigs.k8s.io/cluster-api-provider-vsphere/pkg/conversion/client"
	"sigs.k8s.io/cluster-api-provider-vsphere/pkg/services"
)

// vmOperatorV1Alpha6GroupVersion is the GroupVersion string for vm-operator v1alpha6,
// which is the first version to support IPFamilyPolicy/IPFamilies on VirtualMachineService.
const vmOperatorV1Alpha6GroupVersion = "vmoperator.vmware.com/v1alpha6"

const (
	defaultAPIBindPort                   = 6443
	controlPlaneServiceAPIServerPortName = "apiserver"

	// ClusterSelectorKey is a label we use to store the cluster name on VirtualMachine objects.
	ClusterSelectorKey = "capv.vmware.com/cluster.name"
	nodeSelectorKey    = "capv.vmware.com/cluster.role"
	roleNode           = "node"
	roleControlPlane   = "controlplane"

	// TODO(lubronzhan): Deprecated, will be removed in a future release.
	// https://github.com/kubernetes-sigs/cluster-api-provider-vsphere/issues/1483
	// legacyClusterSelectorKey and legacyNodeSelectorKey are added for backward compatibility.
	// These will be removed in the future release.
	// Please refer to the issue above for deprecation process.
	//
	// Deprecated: legacyClusterSelectorKey will be removed in a future release.
	legacyClusterSelectorKey = "capw.vmware.com/cluster.name"

	// Please refer to the issue above for deprecation process.
	//
	// Deprecated: legacyClusterSelectorKey will be removed in a future release.
	legacyNodeSelectorKey = "capw.vmware.com/cluster.role"
)

// CPService represents the ability to reconcile a ControlPlaneEndpoint.
type CPService struct {
	Client client.Client
}

// ReconcileControlPlaneEndpointService manages the lifecycle of a control plane endpoint managed by a vmoperator VirtualMachineService.
func (s *CPService) ReconcileControlPlaneEndpointService(ctx context.Context, clusterCtx *vmware.ClusterContext, netProvider services.NetworkProvider) (*vmwarev1.APIEndpoint, error) {
	log := ctrl.LoggerFrom(ctx)
	log.V(4).Info("Reconciling control plane VirtualMachineService for cluster")

	if clusterCtx.Cluster == nil {
		return nil, errors.New("cluster is required for control plane endpoint reconciliation")
	}

	// If the NetworkProvider does not support a load balancer, this should be a no-op
	if !netProvider.HasLoadBalancer() {
		return nil, nil
	}

	vmService, err := s.getVMControlPlaneService(ctx, clusterCtx)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			err = errors.Wrapf(err, "failed to check if VirtualMachineService exists")
			deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.LoadBalancerCreationFailedV1Beta1Reason, clusterv1.ConditionSeverityWarning, "%v", err)
			conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
				Type:    vmwarev1.VSphereClusterLoadBalancerReadyCondition,
				Status:  metav1.ConditionFalse,
				Reason:  vmwarev1.VSphereClusterLoadBalancerNotReadyReason,
				Message: err.Error(),
			})
			return nil, err
		}

		// Get the provider annotations for the ControlPlane Service.
		annotations, err := netProvider.GetVMServiceAnnotations(ctx, clusterCtx)
		if err != nil {
			err = errors.Wrapf(err, "failed to get provider VirtualMachineService annotations")
			deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.LoadBalancerCreationFailedV1Beta1Reason, clusterv1.ConditionSeverityWarning, "%v", err)
			conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
				Type:    vmwarev1.VSphereClusterLoadBalancerReadyCondition,
				Status:  metav1.ConditionFalse,
				Reason:  vmwarev1.VSphereClusterLoadBalancerNotReadyReason,
				Message: err.Error(),
			})
			return nil, err
		}

		vmService, err = s.createVMControlPlaneService(ctx, clusterCtx, annotations)
		if err != nil {
			err = errors.Wrapf(err, "failed to create VirtualMachineService")
			deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.LoadBalancerCreationFailedV1Beta1Reason, clusterv1.ConditionSeverityWarning, "%v", err)
			conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
				Type:    vmwarev1.VSphereClusterLoadBalancerReadyCondition,
				Status:  metav1.ConditionFalse,
				Reason:  vmwarev1.VSphereClusterLoadBalancerNotReadyReason,
				Message: err.Error(),
			})
			return nil, err
		}
	} else {
		// Ensure label so runtime extension / TKR webhook can find this VMService by cluster.x-k8s.io/cluster-name (e.g. for dual-stack certSANs).
		if vmService.Labels == nil {
			vmService.Labels = make(map[string]string)
		}
		if vmService.Labels[clusterv1.ClusterNameLabel] != clusterCtx.Cluster.Name {
			vmService.Labels[clusterv1.ClusterNameLabel] = clusterCtx.Cluster.Name
			if err := s.Client.Update(ctx, vmService); err != nil {
				return nil, errors.Wrapf(err, "failed to set cluster name label on VirtualMachineService")
			}
		}
		// Existing service: ensure dual stack spec (IPFamilyPolicy, IPFamilies) is set when cluster is dual stack.
		if err := s.ensureDualStackSpecOnVMService(ctx, clusterCtx, vmService); err != nil {
			return nil, errors.Wrapf(err, "failed to set dual stack spec on VirtualMachineService")
		}
	}

	// See if the LB has VIP(s) assigned.
	ipv4, ipv6, err := getVMServiceVIPs(vmService)
	if err != nil {
		err = errors.Wrapf(err, "VirtualMachineService LB does not yet have VIP assigned")
		deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.WaitingForLoadBalancerIPV1Beta1Reason, clusterv1.ConditionSeverityInfo, "%v", err)
		conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
			Type:    vmwarev1.VSphereClusterLoadBalancerReadyCondition,
			Status:  metav1.ConditionFalse,
			Reason:  vmwarev1.VSphereClusterLoadBalancerWaitingForIPReason,
			Message: err.Error(),
		})
		return nil, err
	}

	dualStack := isDualStackEnabled(clusterCtx.Cluster)
	var primaryVIP string
	var requiredIPs []string

	if dualStack {
		// Dual stack: require both families present in VM Service before setting endpoint.
		if ipv4 == "" || ipv6 == "" {
			err := fmt.Errorf("VirtualMachineService LB must have both IPv4 and IPv6 ingress for dual stack cluster (have IPv4: %v, IPv6: %v)", ipv4 != "", ipv6 != "")
			deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.WaitingForLoadBalancerIPV1Beta1Reason, clusterv1.ConditionSeverityInfo, "%v", err)
			conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
				Type:    vmwarev1.VSphereClusterLoadBalancerReadyV1Beta2Condition,
				Status:  metav1.ConditionFalse,
				Reason:  vmwarev1.VSphereClusterLoadBalancerWaitingForIPV1Beta2Reason,
				Message: err.Error(),
			})
			return nil, err
		}
		// First CIDR's IP family is primary (e.g. IPv4-primary dual stack → use ipv4 as endpoint host).
		primaryVIP = primaryVIPFromClusterNetwork(clusterCtx.Cluster, ipv4, ipv6)
		requiredIPs = []string{ipv4, ipv6}
		// Delay until KCP has both IPs in certSANs and observedGeneration == generation.
		if err := ensureKCPReadyForControlPlaneEndpoint(ctx, s.Client, clusterCtx, requiredIPs); err != nil {
			deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.WaitingForKCPReadyReason, clusterv1.ConditionSeverityInfo, "%v", err)
			conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
				Type:    vmwarev1.VSphereClusterLoadBalancerReadyV1Beta2Condition,
				Status:  metav1.ConditionFalse,
				Reason:  vmwarev1.VSphereClusterLoadBalancerWaitingForKCPV1Beta2Reason,
				Message: err.Error(),
			})
			return nil, fmt.Errorf("%w: %w", services.ErrWaitingForKCPReady, err)
		}
	} else {
		// Single stack: use the primary IP from cluster network (first CIDR's family). No KCP check.
		primaryVIP = primaryVIPFromClusterNetwork(clusterCtx.Cluster, ipv4, ipv6)
		if primaryVIP == "" {
			err := fmt.Errorf("VirtualMachineService LB does not have primary family IP for single stack cluster (cluster network primary family has no ingress yet)")
			deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.WaitingForLoadBalancerIPV1Beta1Reason, clusterv1.ConditionSeverityInfo, "%v", err)
			conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
				Type:    vmwarev1.VSphereClusterLoadBalancerReadyV1Beta2Condition,
				Status:  metav1.ConditionFalse,
				Reason:  vmwarev1.VSphereClusterLoadBalancerWaitingForIPV1Beta2Reason,
				Message: err.Error(),
			})
			return nil, err
		}
	}

	cpEndpoint, err := getAPIEndpointFromVIP(vmService, primaryVIP)
	if err != nil {
		err = errors.Wrapf(err, "VirtualMachineService LB does not have an apiserver endpoint")
		deprecatedv1beta1conditions.MarkFalse(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition, vmwarev1.WaitingForLoadBalancerIPV1Beta1Reason, clusterv1.ConditionSeverityWarning, "%v", err)
		conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
			Type:    vmwarev1.VSphereClusterLoadBalancerReadyCondition,
			Status:  metav1.ConditionFalse,
			Reason:  vmwarev1.VSphereClusterLoadBalancerWaitingForIPReason,
			Message: err.Error(),
		})
		return nil, err
	}

	deprecatedv1beta1conditions.MarkTrue(clusterCtx.VSphereCluster, vmwarev1.LoadBalancerReadyV1Beta1Condition)
	conditions.Set(clusterCtx.VSphereCluster, metav1.Condition{
		Type:   vmwarev1.VSphereClusterLoadBalancerReadyCondition,
		Status: metav1.ConditionTrue,
		Reason: vmwarev1.VSphereClusterLoadBalancerReadyReason,
	})
	return cpEndpoint, nil
}

func controlPlaneVMServiceName(clusterName string) string {
	return clusterName
}

// legacyControlPlaneVMServiceName was used for creating the ControlPlane VirtualMachineService prior to
// v1.13.0. It resulted in limiting the name of a Cluster to 41 characters (besides other places).
func legacyControlPlaneVMServiceName(clusterName string) string {
	return fmt.Sprintf("%s-control-plane-service", clusterName)
}

// ClusterRoleVMLabels returns labels applied to a VirtualMachine in the cluster. The Control Plane
// VM Service uses these labels to select VMs, as does the Cloud Provider.
// Add the legacyNodeSelectorKey and legacyClusterSelectorKey to machines as well.
func clusterRoleVMLabels(ctx *vmware.ClusterContext, controlPlane bool) map[string]string {
	result := map[string]string{
		ClusterSelectorKey:       ctx.Cluster.Name,
		legacyClusterSelectorKey: ctx.Cluster.Name,
	}
	if controlPlane {
		result[nodeSelectorKey] = roleControlPlane
		result[legacyNodeSelectorKey] = roleControlPlane
	} else {
		result[nodeSelectorKey] = roleNode
		result[legacyNodeSelectorKey] = roleNode
	}
	return result
}

func newVirtualMachineService(ctx *vmware.ClusterContext) *vmoprvhub.VirtualMachineService {
	// ClusterNameLabel is required so the runtime extension / TKR webhook can list VirtualMachineService
	// by label (e.g. for dual-stack certSANs). Without it, "VirtualMachineService not found (label and owner lookup)" occurs.
	return &vmoprvhub.VirtualMachineService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      controlPlaneVMServiceName(ctx.Cluster.Name),
			Namespace: ctx.Cluster.Namespace,
			Labels: map[string]string{
				clusterv1.ClusterNameLabel: ctx.Cluster.Name,
			},
		},
	}
}

func (s *CPService) createVMControlPlaneService(ctx context.Context, clusterCtx *vmware.ClusterContext, annotations map[string]string) (*vmoprvhub.VirtualMachineService, error) {
	// Note that the current implementation will only create a VirtualMachineService for a load balanced endpoint
	serviceType := vmoprvhub.VirtualMachineServiceTypeLoadBalancer

	vmService := newVirtualMachineService(clusterCtx)

	vmServiceExists := true
	if err := s.Client.Get(ctx, client.ObjectKeyFromObject(vmService), vmService); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, err
		}
		vmServiceExists = false
	}
	originalVMService := vmService.DeepCopy()

	// Determine the spoke API version: set by the conversion client on a successful Get
	// (existing objects), or queried directly from the converter for new objects where no
	// prior Get populated Source.APIVersion.
	vmServiceAPIVersion := vmService.GetSource().APIVersion
	if vmServiceAPIVersion == "" {
		if gv, err := conversionclient.SpokeGroupVersionFor(s.Client, vmService); err == nil {
			vmServiceAPIVersion = gv.String()
		}
	}

	if vmService.Annotations == nil {
		vmService.Annotations = annotations
	} else {
		for k, v := range annotations {
			vmService.Annotations[k] = v
		}
	}
	vmService.Annotations = annotations
	spec := vmoprvhub.VirtualMachineServiceSpec{
		Type: serviceType,
		Ports: []vmoprvhub.VirtualMachineServicePort{
			{
				Name:       controlPlaneServiceAPIServerPortName,
				Protocol:   "TCP",
				Port:       defaultAPIBindPort,
				TargetPort: defaultAPIBindPort,
			},
		},
		Selector: clusterRoleVMLabels(clusterCtx, true),
	}
	// For dual stack clusters on supervisors that support v1alpha6+: set IPFamilyPolicy to
	// RequireDualStack and IPFamilies (primary family first). Skipped on older supervisors
	// (v1alpha5 and below) where these fields are absent.
	if isDualStackEnabled(clusterCtx.Cluster) && vmServiceSupportsDualStackSpec(vmServiceAPIVersion) {
		spec.IPFamilyPolicy = ptr.To(corev1.IPFamilyPolicyRequireDualStack)
		spec.IPFamilies = clusterNetworkIPFamiliesSlice(clusterCtx.Cluster) // [IPv4, IPv6] or [IPv6, IPv4] by cluster order
	}
	vmService.Spec = spec

	if err := ctrlutil.SetOwnerReference(
		clusterCtx.VSphereCluster,
		vmService,
		s.Client.Scheme(),
	); err != nil {
		return nil, errors.Wrapf(
			err,
			"error setting %s/%s as owner of %s/%s",
			clusterCtx.VSphereCluster.Namespace,
			clusterCtx.VSphereCluster.Name,
			vmService.Namespace,
			vmService.Name,
		)
	}

	if !vmServiceExists {
		if err := s.Client.Create(ctx, vmService); err != nil {
			return nil, err
		}
	} else if !reflect.DeepEqual(originalVMService, vmService) {
		patch, err := conversionclient.MergeFrom(ctx, s.Client, originalVMService)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create patch for VirtualMachineService object")
		}
		if err := s.Client.Patch(ctx, vmService, patch); err != nil {
			return nil, errors.Wrapf(err, "failed to patch VirtualMachineService object")
		}
	}

	return vmService, nil
}

// vmServiceSupportsDualStackSpec reports whether the supervisor's VirtualMachineService
// API supports dual-stack fields (IPFamilyPolicy / IPFamilies).
// These fields were introduced in vm-operator v1alpha6; v1alpha5 and earlier do not have them.
//
// For an existing vmService (fetched via Get), apiVersion is vmService.GetSource().APIVersion.
// For a new vmService (not yet created), pass the spoke version returned by
// conversionclient.SpokeGroupVersionFor.
func vmServiceSupportsDualStackSpec(apiVersion string) bool {
	return apiVersion == vmOperatorV1Alpha6GroupVersion
}

// ensureDualStackSpecOnVMService sets Spec.IPFamilyPolicy and Spec.IPFamilies on the
// VirtualMachineService when the cluster is dual stack (RequireDualStack, [IPv4, IPv6] or [IPv6, IPv4]).
// It is a no-op when the supervisor does not support v1alpha6 (IPFamilyPolicy/IPFamilies absent).
func (s *CPService) ensureDualStackSpecOnVMService(ctx context.Context, clusterCtx *vmware.ClusterContext, vmService *vmoprvhub.VirtualMachineService) error {
	if !isDualStackEnabled(clusterCtx.Cluster) {
		return nil
	}
	if !vmServiceSupportsDualStackSpec(vmService.GetSource().APIVersion) {
		ctrl.LoggerFrom(ctx).V(4).Info("Skipping dual-stack VirtualMachineService spec: supervisor API version does not support IPFamilyPolicy/IPFamilies",
			"apiVersion", vmService.GetSource().APIVersion, "requiredVersion", vmOperatorV1Alpha6GroupVersion)
		return nil
	}
	wantPolicy := ptr.To(corev1.IPFamilyPolicyRequireDualStack)
	wantFamilies := clusterNetworkIPFamiliesSlice(clusterCtx.Cluster)
	if vmService.Spec.IPFamilyPolicy != nil && *vmService.Spec.IPFamilyPolicy == *wantPolicy &&
		slices.Equal(vmService.Spec.IPFamilies, wantFamilies) {
		return nil
	}
	vmService.Spec.IPFamilyPolicy = wantPolicy
	vmService.Spec.IPFamilies = wantFamilies
	return s.Client.Update(ctx, vmService)
}

func (s *CPService) getVMControlPlaneService(ctx context.Context, clusterCtx *vmware.ClusterContext) (*vmoprvhub.VirtualMachineService, error) {
	log := ctrl.LoggerFrom(ctx)

	vmService := &vmoprvhub.VirtualMachineService{}
	vmServiceKey := client.ObjectKey{
		Namespace: clusterCtx.Cluster.Namespace,
		Name:      controlPlaneVMServiceName(clusterCtx.Cluster.Name),
	}
	if err := s.Client.Get(ctx, vmServiceKey, vmService); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get VirtualMachineService %s: %v", vmServiceKey.Name, err)
		}

		// In case of not finding the ControlPlane VirtualMachineService: fallback to try the legacy name.
		fallbackVMServiceKey := client.ObjectKey{
			Namespace: vmServiceKey.Namespace,
			Name:      legacyControlPlaneVMServiceName(clusterCtx.Cluster.Name),
		}
		if fallbackErr := s.Client.Get(ctx, fallbackVMServiceKey, vmService); fallbackErr != nil {
			if !apierrors.IsNotFound(fallbackErr) {
				return nil, fmt.Errorf("failed to get VirtualMachineService %s: %v", fallbackVMServiceKey.Name, fallbackErr)
			}

			log.Info("VirtualMachineService was not found", "VirtualMachineService", klog.KRef(vmServiceKey.Namespace, vmServiceKey.Name))
			return nil, err
		}
	}

	// Verify OwnerReference UID to prevent adopting a service from a
	// previous cluster with the same name.
	refs := vmService.GetOwnerReferences()
	for _, ref := range refs {
		if ref.Kind == "VSphereCluster" && ref.UID != clusterCtx.VSphereCluster.UID {
			return nil, fmt.Errorf("VirtualMachineService %s exists but is owned by a different VSphereCluster instance %s", vmServiceKey.Name, ref.UID)
		}
	}

	// If the service is being deleted, it could be an old service with the same name,
	// return error.
	if !vmService.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("VirtualMachineService %s exists but is being deleted", vmServiceKey.Name)
	}

	return vmService, nil
}

// isDualStackCluster infers dual stack from cluster network: podCIDRs, or if absent serviceCIDRs.
// Dual stack = at least one IPv4 and one IPv6 CIDR. First CIDR's family is treated as primary (e.g. IPv4-primary).
// This is a pure topology check; use isDualStackEnabled to additionally gate on the feature flag.
func isDualStackCluster(cluster *clusterv1.Cluster) bool {
	if cluster == nil {
		return false
	}
	cidrBlocks := cluster.Spec.ClusterNetwork.Pods.CIDRBlocks
	if len(cidrBlocks) == 0 {
		cidrBlocks = cluster.Spec.ClusterNetwork.Services.CIDRBlocks
	}
	if len(cidrBlocks) == 0 {
		return false
	}
	var hasIPv4, hasIPv6 bool
	for _, cidr := range cidrBlocks {
		if strings.Contains(cidr, ":") {
			hasIPv6 = true
		} else {
			hasIPv4 = true
		}
	}
	return hasIPv4 && hasIPv6
}

// isDualStackEnabled returns true only when the cluster is topologically dual stack
// AND the IPv6DualStack feature gate is enabled.
// All dual-stack-specific behavior (IPFamily fields, dual-VIP requirement, KCP certSANs gate)
// must be conditioned on this function so a single feature flag controls the entire feature.
func isDualStackEnabled(cluster *clusterv1.Cluster) bool {
	return feature.Gates.Enabled(feature.IPv6DualStack) && isDualStackCluster(cluster)
}

// getVMServiceVIPs returns IPv4 and IPv6 from the VirtualMachineService LoadBalancer ingress.
// For dual stack we require both families to be present before setting the control plane endpoint.
func getVMServiceVIPs(vmService *vmoprvhub.VirtualMachineService) (ipv4, ipv6 string, err error) {
	if vmService.Spec.Type != vmoprvhub.VirtualMachineServiceTypeLoadBalancer {
		return "", "", fmt.Errorf("VirtualMachineService for control plane does not have load balancer")
	}

	var ipv4Addr, ipv6Addr string
	for _, ingress := range vmService.Status.LoadBalancer.Ingress {
		if ingress.IP != "" {
			if strings.Contains(ingress.IP, ":") {
				if ipv6Addr == "" {
					ipv6Addr = ingress.IP
				}
			} else {
				if ipv4Addr == "" {
					ipv4Addr = ingress.IP
				}
			}
		}
	}

	if ipv4Addr == "" && ipv6Addr == "" {
		return "", "", fmt.Errorf("VirtualMachineService LoadBalancer does not have any Ingresses")
	}
	return ipv4Addr, ipv6Addr, nil
}

// primaryVIPFromClusterNetwork returns the primary VIP (ipv4 or ipv6) based on cluster network:
// first CIDR's family is primary. Used for both single and dual stack.
func primaryVIPFromClusterNetwork(cluster *clusterv1.Cluster, ipv4, ipv6 string) string {
	if cluster == nil {
		return ipv4 // safe default when cluster config unavailable
	}
	cidrBlocks := cluster.Spec.ClusterNetwork.Pods.CIDRBlocks
	if len(cidrBlocks) == 0 {
		cidrBlocks = cluster.Spec.ClusterNetwork.Services.CIDRBlocks
	}
	if len(cidrBlocks) > 0 && strings.Contains(cidrBlocks[0], ":") {
		return ipv6
	}
	return ipv4
}

// clusterNetworkIPFamiliesSlice returns IP families for dual stack strictly in cluster CIDR order.
// Iterates Pods.CIDRBlocks (or Services.CIDRBlocks) in order and adds each family in first-seen order,
// so IPFamilies matches the cluster network order (e.g. [IPv4, IPv6] or [IPv6, IPv4]).
func clusterNetworkIPFamiliesSlice(cluster *clusterv1.Cluster) []corev1.IPFamily {
	if cluster == nil {
		return []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol}
	}
	cidrBlocks := cluster.Spec.ClusterNetwork.Pods.CIDRBlocks
	if len(cidrBlocks) == 0 {
		cidrBlocks = cluster.Spec.ClusterNetwork.Services.CIDRBlocks
	}
	var families []corev1.IPFamily
	seen := make(map[corev1.IPFamily]bool)
	for _, cidr := range cidrBlocks {
		fam := corev1.IPv4Protocol
		if strings.Contains(cidr, ":") {
			fam = corev1.IPv6Protocol
		}
		if !seen[fam] {
			seen[fam] = true
			families = append(families, fam)
		}
	}
	if len(families) == 0 {
		return []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol}
	}
	return families
}

// kcpObservedGeneration returns true if the KCP controller has observed the given generation,
// either via status.ObservedGeneration or any status condition's observedGeneration (some
// controllers set conditions before the top-level ObservedGeneration).
func kcpObservedGeneration(kcp *controlplanev1.KubeadmControlPlane, gen int64) bool {
	if kcp.Status.ObservedGeneration == gen {
		return true
	}
	for _, c := range kcp.Status.Conditions {
		if c.ObservedGeneration == gen {
			return true
		}
	}
	return false
}

// ensureKCPReadyForControlPlaneEndpoint ensures KubeadmControlPlane has (a) requiredIPs in certSANs and
// (b) status.observedGeneration or a condition's observedGeneration == metadata.generation to avoid race between CAPV and KCP controllers.
// This prevents the first control plane machine from being created with stale certSANs (e.g. single stack)
// and avoids an extra rollout for dual stack clusters.
func ensureKCPReadyForControlPlaneEndpoint(ctx context.Context, c client.Client, clusterCtx *vmware.ClusterContext, requiredIPs []string) error {
	if clusterCtx.Cluster == nil || len(requiredIPs) == 0 {
		return nil
	}

	kcpList := &controlplanev1.KubeadmControlPlaneList{}
	if err := c.List(ctx, kcpList,
		client.InNamespace(clusterCtx.Cluster.Namespace),
		client.MatchingLabels{clusterv1.ClusterNameLabel: clusterCtx.Cluster.Name}); err != nil {
		return errors.Wrapf(err, "failed to list KubeadmControlPlane for cluster %s", clusterCtx.Cluster.Name)
	}
	if len(kcpList.Items) == 0 {
		// For dual stack we must not set endpoint until KCP exists and has both IPs in certSANs.
		if len(requiredIPs) > 1 {
			return fmt.Errorf("KubeadmControlPlane not found for dual stack cluster %s, cannot set control plane endpoint until KCP exists with both IPs in certSANs", clusterCtx.Cluster.Name)
		}
		// Single stack: no KCP yet (e.g. topology not created); allow endpoint to be set.
		return nil
	}
	if len(kcpList.Items) > 1 {
		return fmt.Errorf("multiple KubeadmControlPlane objects found for cluster %s, expected 1", clusterCtx.Cluster.Name)
	}

	kcp := &kcpList.Items[0]
	if !kcp.GetDeletionTimestamp().IsZero() {
		return nil
	}

	// Re-fetch KCP to get latest status (observedGeneration is updated by KCP controller asynchronously).
	if err := c.Get(ctx, client.ObjectKeyFromObject(kcp), kcp); err != nil {
		return errors.Wrapf(err, "failed to get KubeadmControlPlane %s/%s", kcp.Namespace, kcp.Name)
	}

	// (b) Avoid race: KCP controller must have reconciled the current spec. Accept either
	// status.ObservedGeneration or any condition's observedGeneration (controller may set conditions first).
	if !kcpObservedGeneration(kcp, kcp.GetGeneration()) {
		return fmt.Errorf("KubeadmControlPlane %s/%s observedGeneration %d does not match generation %d",
			kcp.Namespace, kcp.Name, kcp.Status.ObservedGeneration, kcp.GetGeneration())
	}

	// (a) For dual stack, certSANs must contain all required IPs (both primary and secondary LB VIPs) so the first machine gets both in certs.
	// The runtime extension must add both IPs to apiServer.certSANs; adding only the secondary-family IP is insufficient.
	if len(requiredIPs) > 1 {
		certSANs := getKCPCertSANs(kcp)
		for _, ip := range requiredIPs {
			if !slices.Contains(certSANs, ip) {
				return fmt.Errorf("KubeadmControlPlane %s/%s certSANs must contain %q for dual stack (have: %v); ensure runtime extension adds both primary and secondary LB IPs to apiServer.certSANs",
					kcp.Namespace, kcp.Name, ip, certSANs)
			}
		}
	}
	return nil
}

func getKCPCertSANs(kcp *controlplanev1.KubeadmControlPlane) []string {
	if kcp.Spec.KubeadmConfigSpec.ClusterConfiguration.APIServer.CertSANs != nil {
		return kcp.Spec.KubeadmConfigSpec.ClusterConfiguration.APIServer.CertSANs
	}
	return nil
}

func getAPIEndpointFromVIP(vmService *vmoprvhub.VirtualMachineService, vip string) (*vmwarev1.APIEndpoint, error) {
	name := controlPlaneServiceAPIServerPortName
	servicePort := int32(-1)
	for _, port := range vmService.Spec.Ports {
		if port.Name == name && port.Protocol == "TCP" {
			servicePort = port.Port
			break
		}
	}

	if servicePort == -1 {
		return nil, fmt.Errorf("VirtualMachineService does not have port entry for %q", name)
	}

	return &vmwarev1.APIEndpoint{
		Host: vip,
		Port: servicePort,
	}, nil
}
