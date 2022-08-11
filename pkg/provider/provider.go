package provider

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"os"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	cloudprovider "k8s.io/cloud-provider"
)

// OutSideCluster allows the controller to be started using a local kubeConfig for testing
var OutSideCluster bool

// ProviderType specifies if service creation happens inside current cluster or in another one
var ProviderType KubeVipProviderType

// DefaultProviderType is the name of the default provider type
var DefaultProviderType = KubeVipInternalProvider

const (
	//ProviderName is the name of the cloud provider
	ProviderName = "kubevip"

	//KubeVipCloudConfig is the default name of the load balancer config Map
	KubeVipCloudConfig = "kubevip"

	//KubeVipClientConfig is the default name of the load balancer config Map
	KubeVipClientConfig = "kubevip"

	//KubeVipUpstreamConfig is the default name of the upstream cluster config Map
	KubeVipUpstreamConfig = "provider-config"

	//KubeVipUpstreamNamespace is the default name of the upstream cluster config Map namespace
	KubeVipUpstreamNamespace = "kube-system"

	//KubeVipUpstreamClusterKey is the default name of the key for configuring cluster name in provider config Map
	KubeVipUpstreamClusterKey = "cluster"

	//KubeVipUpstreamConfigKey is the default name of the key for configuring cluster name in provider config Map
	KubeVipUpstreamConfigKey = "config"

	//KubeVipServicesKey is the key in the ConfigMap that has the services configuration
	KubeVipServicesKey = "kubevip-services"

	//KubeVipInternalProvider represents value for provider type internal
	KubeVipInternalProvider = "internal"

	//KubeVipExternalProvider represents value for provider type external
	KubeVipExternalProvider = "external"
)

func init() {
	cloudprovider.RegisterCloudProvider(ProviderName, newKubeVipCloudProvider)
}

// KubeVipCloudProvider - contains all of the interfaces for the cloud provider
type KubeVipCloudProvider struct {
	lb cloudprovider.LoadBalancer
}

type KubeVipProviderType string

var _ cloudprovider.Interface = &KubeVipCloudProvider{}

func newKubeVipCloudProvider(io.Reader) (cloudprovider.Interface, error) {
	ns := os.Getenv("KUBEVIP_NAMESPACE")
	cm := os.Getenv("KUBEVIP_CONFIG_MAP")
	kubeconfig := os.Getenv("KUBECONFIG")

	if cm == "" {
		cm = KubeVipCloudConfig
	}

	if ns == "" {
		ns = "default"
	}

	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	var cl *kubernetes.Clientset

	if !OutSideCluster {
		// This will attempt to load the configuration when running within a POD
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes client config: %s", err.Error())
		}
		cl, err = kubernetes.NewForConfig(cfg)

		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes client: %s", err.Error())
		}
		// use the current context in kubeconfig
	} else {
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			panic(err.Error())
		}
		cl, err = kubernetes.NewForConfig(config)

		if err != nil {
			return nil, fmt.Errorf("error creating kubernetes client: %s", err.Error())
		}
	}

	return &KubeVipCloudProvider{
		lb: newLoadBalancer(cl, ns, cm),
	}, nil
}

// Initialize - starts the clound-provider controller
func (p *KubeVipCloudProvider) Initialize(clientBuilder cloudprovider.ControllerClientBuilder, stop <-chan struct{}) {
	clientset := clientBuilder.ClientOrDie("do-shared-informers")
	sharedInformer := informers.NewSharedInformerFactory(clientset, 0)

	//res := NewResourcesController(c.resources, sharedInformer.Core().V1().Services(), clientset)

	sharedInformer.Start(nil)
	sharedInformer.WaitForCacheSync(nil)
	//go res.Run(stop)
	//go c.serveDebug(stop)
}

// LoadBalancer returns a loadbalancer interface. Also returns true if the interface is supported, false otherwise.
func (p *KubeVipCloudProvider) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	return p.lb, true
}

// ProviderName returns the cloud provider ID.
func (p *KubeVipCloudProvider) ProviderName() string {
	return ProviderName
}

// implement an own simple flag type for provider type...
// String is used both by fmt.Print and by Cobra in help text
func (p *KubeVipProviderType) String() string {
	if string(*p) == "" {
		return string(DefaultProviderType)
	}
	return string(*p)
}

// Set must have pointer receiver so it doesn't change the value of a copy
func (p *KubeVipProviderType) Set(v string) error {
	switch v {
	case "internal", "external":
		*p = KubeVipProviderType(v)
		return nil
	default:
		return errors.New(`must be one of "internal" or "external"`)
	}
}

// Type is only used in help text
func (p *KubeVipProviderType) Type() string {
	return "KubeVipProviderType"
}
