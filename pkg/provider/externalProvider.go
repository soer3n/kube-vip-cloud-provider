package provider

import (
	"context"
	"errors"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog"
)

//kubevipExternalLoadBalancerManager -
type kubevipExternalLoadBalancerManager struct {
	kubeClient        *kubernetes.Clientset
	providerClient    *kubernetes.Clientset
	nameSpace         string
	upstreamNamespace string
}

func newExternalLoadBalancerProvider(kubeClient *kubernetes.Clientset, ns string) cloudprovider.LoadBalancer {

	k := &kubevipExternalLoadBalancerManager{
		kubeClient: kubeClient,
		nameSpace:  ns,
	}

	if err := k.setProviderData(kubeClient); err != nil {
		return nil
	}

	return k
}

func (k *kubevipExternalLoadBalancerManager) EnsureLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (lbs *v1.LoadBalancerStatus, err error) {
	return k.syncExternalLoadBalancer(ctx, service, nodes)
}
func (k *kubevipExternalLoadBalancerManager) UpdateLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, nodes []*v1.Node) (err error) {
	_, err = k.syncExternalLoadBalancer(ctx, service, nodes)
	return err
}

func (k *kubevipExternalLoadBalancerManager) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *v1.Service) error {
	return k.deleteUpstreamLoadBalancer(ctx, service)
}

func (k *kubevipExternalLoadBalancerManager) GetLoadBalancer(ctx context.Context, clusterName string, service *v1.Service) (status *v1.LoadBalancerStatus, exists bool, err error) {
	if service.Labels["implementation"] == "kube-vip" {
		return &service.Status.LoadBalancer, true, nil
	}
	return nil, false, nil
}

// GetLoadBalancerName returns the name of the load balancer. Implementations must treat the
// *v1.Service parameter as read-only and not modify it.
func (k *kubevipExternalLoadBalancerManager) GetLoadBalancerName(_ context.Context, clusterName string, service *v1.Service) string {
	return getDefaultLoadBalancerName(service)
}

//nolint
func (k *kubevipExternalLoadBalancerManager) deleteUpstreamLoadBalancer(ctx context.Context, service *v1.Service) error {

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

func (k *kubevipExternalLoadBalancerManager) syncExternalLoadBalancer(ctx context.Context, service *v1.Service, nodes []*v1.Node) (*v1.LoadBalancerStatus, error) {

	klog.Infoln("sync service in upstream cluster")
	loadBalancerIP, err := k.syncUpstreamService(ctx, service)

	if err != nil {
		return nil, err
	}

	klog.Infoln("update service in downstream cluster")
	if err = k.updateServiceObject(ctx, service, loadBalancerIP); err != nil {
		return nil, err
	}

	klog.Infoln("syncing endpoints in upstream cluster")
	if err = k.syncUpstreamEndpoints(ctx, service, nodes); err != nil {
		return &service.Status.LoadBalancer, err
	}

	return &v1.LoadBalancerStatus{
		Ingress: []v1.LoadBalancerIngress{
			{
				IP: loadBalancerIP,
			},
		},
	}, nil

}

func (k *kubevipExternalLoadBalancerManager) syncUpstreamService(ctx context.Context, service *v1.Service) (string, error) {
	var loadBalancerIP string
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// TODO:
		// step 1: check if upstream service already exists
		// step 2: create if needed
		// step 3: wait until an ip address is allocated
		// step 4: return allocated ip

		recentService, getErr := k.providerClient.CoreV1().Services(k.upstreamNamespace).Get(ctx, service.Name, metav1.GetOptions{})

		if getErr != nil {
			if !k8serrors.IsNotFound(getErr) {
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
					Ports: []v1.ServicePort{
						{},
					},
				},
			}

			newPorts := []v1.ServicePort{}
			for _, port := range service.Spec.Ports {
				newPorts = append(newPorts, v1.ServicePort{
					Name:     port.Name,
					Port:     port.Port,
					Protocol: v1.ProtocolTCP,
				})
			}

			recentService.Spec.Ports = newPorts
			recentService, createErr := k.providerClient.CoreV1().Services(k.upstreamNamespace).Create(ctx, recentService, metav1.CreateOptions{})

			if createErr != nil {
				return createErr
			}

			klog.Infof("new svc object: %v", recentService.Name)

			w, _ := k.providerClient.CoreV1().Services(k.upstreamNamespace).Watch(ctx, metav1.ListOptions{
				FieldSelector: "metadata.name=" + service.Name,
			})

			klog.Infof("waiting for ip allocation for svc object: %v", recentService.Name)
			loadBalancerIP = waitForServiceIPAllocation(w)
		} else {

			klog.Infof("svc object: %v", recentService.Name)
			loadBalancerIP = recentService.Spec.LoadBalancerIP

			if !reflect.DeepEqual(service.Spec.Ports, recentService.Spec.Ports) {
				klog.Infoln("updating service object" + service.Name + "in upstream cluster")
				// Update the actual service with the address and the labels
				newPorts := []v1.ServicePort{}
				for _, port := range service.Spec.Ports {
					newPorts = append(newPorts, v1.ServicePort{
						Name:     port.Name,
						Port:     port.Port,
						Protocol: v1.ProtocolTCP,
					})
				}

				recentService.Spec.Ports = newPorts

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

func (k *kubevipExternalLoadBalancerManager) syncUpstreamEndpoints(ctx context.Context, service *v1.Service, nodes []*v1.Node) error {

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

		for _, node := range nodes {
			if !node.Spec.Unschedulable {
				newEndpoints.Subsets[0].Addresses = append(newEndpoints.Subsets[0].Addresses, v1.EndpointAddress{
					IP:       getExternalIPFromNodeObject(*node),
					Hostname: node.ObjectMeta.Annotations["kubernetes.io/hostname"],
					NodeName: &node.Name,
				})
			} else {
				newEndpoints.Subsets[0].NotReadyAddresses = append(newEndpoints.Subsets[0].NotReadyAddresses, v1.EndpointAddress{
					IP:       getExternalIPFromNodeObject(*node),
					Hostname: node.ObjectMeta.Annotations["kubernetes.io/hostname"],
					NodeName: &node.Name,
				})
			}
		}

		for _, port := range service.Spec.Ports {
			newEndpoints.Subsets[0].Ports = append(newEndpoints.Subsets[0].Ports, v1.EndpointPort{
				Name:     port.Name,
				Protocol: v1.ProtocolTCP,
				Port:     port.NodePort,
			})
		}

		if getErr != nil {
			if !k8serrors.IsNotFound(getErr) {
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

func (k *kubevipExternalLoadBalancerManager) updateServiceObject(ctx context.Context, service *v1.Service, loadBalancerIP string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		recentService, getErr := k.kubeClient.CoreV1().Services(service.Namespace).Get(ctx, service.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}

		if service.Spec.LoadBalancerIP == "" {
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
		}

		return nil

	})
}

func (k *kubevipExternalLoadBalancerManager) setProviderData(cl *kubernetes.Clientset) error {

	// load external k8s config for communicating with glbs cluster
	klog.Infoln("loading config for upstream kubernetes cluster")

	opts := metav1.GetOptions{}
	externalProviderConfig, err := cl.CoreV1().Secrets(KubeVipUpstreamNamespace).Get(context.Background(), KubeVipUpstreamConfig, opts)

	if err != nil {
		klog.Errorf("could not load provider config")
		return err
	}

	klog.Infof("config loaded succesfully. Building client...")

	if _, ok := externalProviderConfig.Data[KubeVipUpstreamClusterKey]; !ok {
		return errors.New("cluster key not found in secret")
	}

	k.upstreamNamespace = string(externalProviderConfig.Data[KubeVipUpstreamClusterKey])
	pc, err := buildClientSetFromSecret(externalProviderConfig)
	if err != nil {
		klog.Errorln("could not build upstream client")
		return err
	}

	k.providerClient = pc
	klog.Infoln("provider client successfully initialized")

	return nil
}

func buildClientSetFromSecret(externalProviderConfig *v1.Secret) (*kubernetes.Clientset, error) {

	var config clientcmd.ClientConfig

	if _, ok := externalProviderConfig.Data[KubeVipUpstreamConfigKey]; !ok {
		return nil, errors.New("config key not found in secret")
	}

	config, err := clientcmd.NewClientConfigFromBytes(externalProviderConfig.Data[KubeVipUpstreamConfigKey])

	if err != nil {
		klog.Errorf("could not transform provider config")
		return nil, err
	}

	restConfig, err := config.ClientConfig()

	if err != nil {
		klog.Errorf("could not build rest client from provider config")
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(restConfig)

	if err != nil {
		klog.Errorf("could not build kubernetes client set from provider config")
		return clientset, err
	}

	return clientset, nil
}

// we need to wait until an ip is allocated in the upstream cluster due to not triggering any event in current downstream cluster
func waitForServiceIPAllocation(w watch.Interface) string {

	defer w.Stop()

	for {
		select {
		case <-time.After(5 * time.Second):
			klog.Error("timeout on getting ip from upstream cluster...")
			return ""
		default:
			e := <-w.ResultChan()
			svc := e.Object.(*v1.Service)
			klog.Infof("got event for service object %v. Event %v", svc.Name, e.Type)

			if svc.Spec.LoadBalancerIP != "" {
				return svc.Spec.LoadBalancerIP
			}
		}
	}
}

func getExternalIPFromNodeObject(node v1.Node) string {

	for _, address := range node.Status.Addresses {
		if address.Type == v1.NodeExternalIP {
			return address.Address
		}
	}

	return ""
}
