package provider

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/kube-vip/kube-vip-cloud-provider/pkg/ipam"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	cloudprovider "k8s.io/cloud-provider"
	cloudproviderapi "k8s.io/cloud-provider/api"

	"k8s.io/klog"
)

//kubevipLoadBalancerManager -
type kubevipLoadBalancerManager struct {
	kubeClient        *kubernetes.Clientset
	providerClient    *kubernetes.Clientset
	nodes             map[string]*v1.Node
	nameSpace         string
	upstreamNamespace string
	cloudConfigMap    string
}

func newLoadBalancer(kubeClient, providerClient *kubernetes.Clientset, ns, upstreamNs, cm string) cloudprovider.LoadBalancer {
	k := &kubevipLoadBalancerManager{
		kubeClient:        kubeClient,
		providerClient:    providerClient,
		nameSpace:         ns,
		upstreamNamespace: upstreamNs,
		nodes:             make(map[string]*v1.Node),
		cloudConfigMap:    cm,
	}
	return k
}

func (k *kubevipLoadBalancerManager) EnsureLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (lbs *v1.LoadBalancerStatus, err error) {

	if ExternalProvider {
		return k.syncExternalLoadBalancer(ctx, service)
	}

	return k.syncLoadBalancer(ctx, service)
}
func (k *kubevipLoadBalancerManager) UpdateLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (err error) {

	if ExternalProvider {
		_, err = k.syncExternalLoadBalancer(ctx, service)
		return err
	}

	_, err = k.syncLoadBalancer(ctx, service)
	return err
}

func (k *kubevipLoadBalancerManager) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *v1.Service) error {

	if ExternalProvider {
		return k.deleteUpstreamLoadBalancer(ctx, service)
	}

	return k.deleteLoadBalancer(ctx, service)
}

func (k *kubevipLoadBalancerManager) GetLoadBalancer(ctx context.Context, clusterName string, service *v1.Service) (status *v1.LoadBalancerStatus, exists bool, err error) {
	if service.Labels["implementation"] == "kube-vip" {
		return &service.Status.LoadBalancer, true, nil
	}
	return nil, false, nil
}

// GetLoadBalancerName returns the name of the load balancer. Implementations must treat the
// *v1.Service parameter as read-only and not modify it.
func (k *kubevipLoadBalancerManager) GetLoadBalancerName(_ context.Context, clusterName string, service *v1.Service) string {
	return getDefaultLoadBalancerName(service)
}

func getDefaultLoadBalancerName(service *v1.Service) string {
	return cloudprovider.DefaultLoadBalancerName(service)
}

//nolint
func (k *kubevipLoadBalancerManager) deleteLoadBalancer(ctx context.Context, service *v1.Service) error {
	klog.Infof("deleting service '%s' (%s)", service.Name, service.UID)

	return nil
}

//nolint
func (k *kubevipLoadBalancerManager) deleteUpstreamLoadBalancer(ctx context.Context, service *v1.Service) error {

	klog.Infof("deleting upstream service '%s' (%s)", service.Name, service.UID)
	if err := k.providerClient.CoreV1().Services(k.upstreamNamespace).Delete(ctx, service.Name, *metav1.NewDeleteOptions(0)); err != nil {
		klog.Error(err)
	}

	klog.Infof("deleting upstream endpoint '%s' (%s)", service.Name, service.UID)
	if err := k.providerClient.CoreV1().Endpoints(k.upstreamNamespace).Delete(ctx, service.Name, *metav1.NewDeleteOptions(0)); err != nil {
		klog.Error(err)
	}

	return nil
}

// syncLoadBalancer
// 1. Is this loadBalancer already created, and does it have an address? return status
// 2. Is this a new loadBalancer (with no IP address)
// 2a. Get all existing kube-vip services
// 2b. Get the network configuration for this service (namespace) / (CIDR/Range)
// 2c. Between the two find a free address

func (k *kubevipLoadBalancerManager) syncLoadBalancer(ctx context.Context, service *v1.Service) (*v1.LoadBalancerStatus, error) {
	// This function reconciles the load balancer state
	klog.Infof("syncing service '%s' (%s)", service.Name, service.UID)

	// The loadBalancer address has already been populated
	if service.Spec.LoadBalancerIP != "" {
		return &service.Status.LoadBalancer, nil
	}

	// Get the clound controller configuration map
	controllerCM, err := k.GetConfigMap(ctx, KubeVipClientConfig, "kube-system")
	if err != nil {
		klog.Errorf("Unable to retrieve kube-vip ipam config from configMap [%s] in kube-system", KubeVipClientConfig)
		// TODO - determine best course of action, create one if it doesn't exist
		controllerCM, err = k.CreateConfigMap(ctx, KubeVipClientConfig, "kube-system")
		if err != nil {
			return nil, err
		}
	}

	// Get ip pool from configmap and determine if it is namespace specific or global
	pool, global, err := discoverPool(controllerCM, service.Namespace, k.cloudConfigMap)

	if err != nil {
		return nil, err
	}

	// Get all services in this namespace or globally, that have the correct label
	var svcs *v1.ServiceList
	if global {
		svcs, err = k.kubeClient.CoreV1().Services("").List(ctx, metav1.ListOptions{LabelSelector: "implementation=kube-vip"})
		if err != nil {
			return &service.Status.LoadBalancer, err
		}
	} else {
		svcs, err = k.kubeClient.CoreV1().Services(service.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "implementation=kube-vip"})
		if err != nil {
			return &service.Status.LoadBalancer, err
		}
	}

	var existingServiceIPS []string
	for x := range svcs.Items {
		existingServiceIPS = append(existingServiceIPS, svcs.Items[x].Labels["ipam-address"])
	}

	// If the LoadBalancer address is empty, then do a local IPAM lookup
	loadBalancerIP, err := discoverAddress(service.Namespace, pool, existingServiceIPS)

	if err != nil {
		return nil, err
	}

	// Update the services with this new address
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		recentService, getErr := k.kubeClient.CoreV1().Services(service.Namespace).Get(ctx, service.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}

		klog.Infof("Updating service [%s], with load balancer IPAM address [%s]", service.Name, loadBalancerIP)

		if recentService.Labels == nil {
			// Just because ..
			recentService.Labels = make(map[string]string)
		}
		// Set Label for service lookups
		recentService.Labels["implementation"] = "kube-vip"
		recentService.Labels["ipam-address"] = loadBalancerIP

		// Set IPAM address to Load Balancer Service
		recentService.Spec.LoadBalancerIP = loadBalancerIP

		// Update the actual service with the address and the labels
		_, updateErr := k.kubeClient.CoreV1().Services(recentService.Namespace).Update(ctx, recentService, metav1.UpdateOptions{})
		return updateErr
	})
	if retryErr != nil {
		return nil, fmt.Errorf("error updating Service Spec [%s] : %v", service.Name, err)
	}

	return &service.Status.LoadBalancer, nil
}

func discoverPool(cm *v1.ConfigMap, namespace, configMapName string) (pool string, global bool, err error) {
	var cidr, ipRange string
	var ok bool

	// Find Cidr
	cidrKey := fmt.Sprintf("cidr-%s", namespace)
	// Lookup current namespace
	if cidr, ok = cm.Data[cidrKey]; !ok {
		klog.Info(fmt.Errorf("no cidr config for namespace [%s] exists in key [%s] configmap [%s]", namespace, cidrKey, configMapName))
		// Lookup global cidr configmap data
		if cidr, ok = cm.Data["cidr-global"]; !ok {
			klog.Info(fmt.Errorf("no global cidr config exists [cidr-global]"))
		} else {
			klog.Infof("Taking address from [cidr-global] pool")
			return cidr, true, nil
		}
	} else {
		klog.Infof("Taking address from [%s] pool", cidrKey)
		return cidr, false, nil
	}

	// Find Range
	rangeKey := fmt.Sprintf("range-%s", namespace)
	// Lookup current namespace
	if ipRange, ok = cm.Data[rangeKey]; !ok {
		klog.Info(fmt.Errorf("no range config for namespace [%s] exists in key [%s] configmap [%s]", namespace, rangeKey, configMapName))
		// Lookup global range configmap data
		if ipRange, ok = cm.Data["range-global"]; !ok {
			klog.Info(fmt.Errorf("no global range config exists [range-global]"))
		} else {
			klog.Infof("Taking address from [range-global] pool")
			return ipRange, true, nil
		}
	} else {
		klog.Infof("Taking address from [%s] pool", rangeKey)
		return ipRange, false, nil
	}

	return "", false, fmt.Errorf("no address pools could be found")
}

func discoverAddress(namespace, pool string, existingServiceIPS []string) (vip string, err error) {
	// Check if ip pool contains a cidr, if not assume it is a range
	if strings.Contains(pool, "/") {
		vip, err = ipam.FindAvailableHostFromCidr(namespace, pool, existingServiceIPS)
		if err != nil {
			return "", err
		}
	} else {
		vip, err = ipam.FindAvailableHostFromRange(namespace, pool, existingServiceIPS)
		if err != nil {
			return "", err
		}
	}

	return vip, err
}

func (k *kubevipLoadBalancerManager) syncExternalLoadBalancer(ctx context.Context, service *v1.Service) (*v1.LoadBalancerStatus, error) {

	if err := k.updateNodeList(ctx); err != nil {
		return &service.Status.LoadBalancer, err
	}

	klog.Infoln("sync service in upstream cluster")
	loadBalancerIP, err := k.syncUpstreamService(service, ctx)

	if err != nil {
		return nil, err
	}

	service.Status.LoadBalancer = v1.LoadBalancerStatus{
		Ingress: []v1.LoadBalancerIngress{
			{
				IP: loadBalancerIP,
			},
		},
	}

	if err = k.updateServiceObject(k.kubeClient, ctx, service, loadBalancerIP); err != nil {
		return nil, err
	}

	klog.Infoln("syncing endpoints in upstream cluster")
	if err = k.syncUpstreamEndpoints(service, ctx); err != nil {
		return &service.Status.LoadBalancer, err
	}

	return &service.Status.LoadBalancer, nil

}

func (k *kubevipLoadBalancerManager) syncUpstreamService(service *v1.Service, ctx context.Context) (string, error) {
	var loadBalancerIP string
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// TODO:
		// step 1: check if upstream service already exists
		// step 2: create if needed
		// step 3: wait until an ip address is allocated
		// step 4: return allocated ip

		recentService, getErr := k.providerClient.CoreV1().Services(k.upstreamNamespace).Get(ctx, service.Name, metav1.GetOptions{})

		if getErr != nil {
			if !errors.IsNotFound(getErr) {
				return getErr
			}

			recentService = &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      service.Name,
					Namespace: k.upstreamNamespace,
				},
				Spec: v1.ServiceSpec{
					Type:            v1.ServiceTypeLoadBalancer,
					SessionAffinity: v1.ServiceAffinityNone,
					Ports:           service.Spec.Ports,
				},
			}

			recentService, createErr := k.providerClient.CoreV1().Services(k.upstreamNamespace).Create(ctx, recentService, metav1.CreateOptions{})

			if createErr != nil {
				return createErr
			}

			klog.Infof("new svc object: %v", recentService)

			w, _ := k.providerClient.CoreV1().Services(k.upstreamNamespace).Watch(ctx, metav1.ListOptions{
				FieldSelector: "metadata.name=" + service.Name,
			})

			loadBalancerIP = waitForIPAllocation(w)
		} else {
			klog.Infof("svc object: %v", recentService)
			klog.Infof("load balancer ip %v", recentService.Spec.LoadBalancerIP)
			loadBalancerIP = recentService.Spec.LoadBalancerIP
			if !reflect.DeepEqual(service.Spec.Ports, recentService.Spec.Ports) {
				klog.Infoln("updating service object %v in upstream cluster", service.Name)
				// Update the actual service with the address and the labels
				recentService.Spec.Ports = service.Spec.Ports

				if recentService.Labels == nil {
					// Just because ..
					recentService.Labels = make(map[string]string)
				}
				// Set Label for service lookups
				recentService.Labels["implementation"] = "kube-vip"
				recentService.Labels["ipam-address"] = recentService.Spec.LoadBalancerIP

				_, updateErr := k.providerClient.CoreV1().Services(recentService.Namespace).Update(ctx, recentService, metav1.UpdateOptions{})

				if updateErr != nil {
					return updateErr
				}
			}
		}
		return nil
	})

	return loadBalancerIP, err
}

func (k *kubevipLoadBalancerManager) syncUpstreamEndpoints(service *v1.Service, ctx context.Context) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {

		recentEndpoints, getErr := k.providerClient.CoreV1().Endpoints(k.upstreamNamespace).Get(ctx, service.Name, metav1.GetOptions{})

		newEndpoints := &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name:      service.Name,
				Namespace: k.upstreamNamespace,
			},
			Subsets: []v1.EndpointSubset{
				{},
			},
		}

		for _, node := range k.nodes {
			newEndpoints.Subsets[0].Addresses = append(newEndpoints.Subsets[0].Addresses, v1.EndpointAddress{
				IP:       getExternalIPFromNodeObject(*node),
				Hostname: node.ObjectMeta.Annotations["kubernetes.io/hostname"],
				NodeName: &node.Name,
			})
		}

		for _, port := range service.Spec.Ports {
			newEndpoints.Subsets[0].Ports = append(newEndpoints.Subsets[0].Ports, v1.EndpointPort{
				Name:     port.Name,
				Protocol: v1.ProtocolTCP,
				Port:     port.NodePort,
			})
		}

		if getErr != nil {
			if !errors.IsNotFound(getErr) {
				return getErr
			}

			recentEndpoints = &v1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:      service.Name,
					Namespace: k.upstreamNamespace,
				},
				Subsets: []v1.EndpointSubset{
					{
						Addresses: newEndpoints.Subsets[0].Addresses,
						Ports:     newEndpoints.Subsets[0].Ports,
					},
				},
			}

			_, createErr := k.providerClient.CoreV1().Endpoints(k.upstreamNamespace).Create(ctx, recentEndpoints, metav1.CreateOptions{})

			if createErr != nil {
				return createErr
			}
		}

		if !reflect.DeepEqual(recentEndpoints.Subsets, newEndpoints.Subsets) {
			recentEndpoints.Subsets = newEndpoints.Subsets
			_, updateErr := k.providerClient.CoreV1().Endpoints(k.upstreamNamespace).Update(ctx, recentEndpoints, metav1.UpdateOptions{})

			if updateErr != nil {
				return updateErr
			}
		}

		return nil

	})
}

func (k *kubevipLoadBalancerManager) updateServiceObject(cl *kubernetes.Clientset, ctx context.Context, service *v1.Service, loadBalancerIP string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		recentService, getErr := cl.CoreV1().Services(service.Namespace).Get(ctx, service.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}

		klog.Infof("Updating service [%s], with load balancer IPAM address [%s]", service.Name, loadBalancerIP)

		if recentService.Labels == nil {
			// Just because ..
			recentService.Labels = make(map[string]string)
		}
		// Set Label for service lookups
		recentService.Labels["implementation"] = "kube-vip"
		recentService.Labels["ipam-address"] = loadBalancerIP

		// Set IPAM address to Load Balancer Service
		recentService.Spec.LoadBalancerIP = loadBalancerIP

		// Update the actual service with the address and the labels
		_, updateErr := cl.CoreV1().Services(recentService.Namespace).Update(ctx, recentService, metav1.UpdateOptions{})

		return updateErr
	})
}

func (k *kubevipLoadBalancerManager) updateNodeList(ctx context.Context) error {

	opts := metav1.ListOptions{}
	currentNodes, err := k.kubeClient.CoreV1().Nodes().List(ctx, opts)

	if err != nil {
		return err
	}

	for _, node := range k.nodes {
		exists := false
		for _, currentNode := range currentNodes.Items {
			if node.Name == currentNode.Name {
				exists = true
			}
		}

		if !exists {
			klog.Infof("removing node %v with provider id %v and addresses %v from load balancer endpoint list because it no longer exists.", node.Name, node.Spec.ProviderID, node.Status.Addresses)
			delete(k.nodes, node.Name)
		}
	}

	for _, node := range currentNodes.Items {
		ip := getExternalIPFromNodeObject(node)

		if node.Spec.Unschedulable {

			if _, ok := k.nodes[node.Name]; ok {
				klog.Infof("removing %v with provided ip %v from load balancer endpoint list", node.Name, ip)
				delete(k.nodes, node.Name)
			}

			klog.Infof("node %v with provided ip %v is not in load balancer endpoint list", node.Name, ip)
			return nil
		}

		if _, ok := k.nodes[node.Name]; ok {
			klog.Infof("updated node %v already in list", node.Name)
			return nil
		}

		w, err := k.kubeClient.CoreV1().Nodes().Watch(context.TODO(), metav1.ListOptions{
			FieldSelector: "metadata.name=" + node.Name,
		})

		if err != nil {
			return err
		}

		klog.Infof("waiting for readiness by external provisioner of node %v", node.Name)

		updatedNode := k.waitForNodeIPAllocation(w)
		externalIP := getExternalIPFromNodeObject(*updatedNode)

		klog.Infof("adding %v with provided id %v and addresses %v to load balancer endpoint list. Using %v as endpoint address.", updatedNode.Name, updatedNode.Spec.ProviderID, updatedNode.Status.Addresses, externalIP)
		k.nodes[node.Name] = updatedNode
	}

	return nil
}

func (k kubevipLoadBalancerManager) waitForNodeIPAllocation(w watch.Interface) *v1.Node {

	defer w.Stop()

	for {
		e := <-w.ResultChan()

		node := e.Object.(*v1.Node)
		klog.Infof("got event %v for node object: %v", e.Type, node.Name)

		if _, ok := node.ObjectMeta.Annotations[cloudproviderapi.TaintExternalCloudProvider]; ok {
			klog.Infof("waiting for provider initalization for node object: %v", node.Name)
			continue
		}

		for _, address := range node.Status.Addresses {
			if address.Type == v1.NodeExternalIP {
				return node
			}
		}
	}
}
