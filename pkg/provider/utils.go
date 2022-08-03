package provider

import (
	"context"
	"errors"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

func getExternalProviderData(cl *kubernetes.Clientset) (*kubernetes.Clientset, string, error) {

	// load external k8s config for communicating with glbs cluster
	klog.Infoln("loading config for upstream kubernetes cluster")

	cluster := ""
	opts := metav1.GetOptions{}
	externalProviderConfig, err := cl.CoreV1().Secrets(KubeVipUpstreamNamespace).Get(context.Background(), KubeVipUpstreamConfig, opts)

	if _, ok := externalProviderConfig.Data[KubeVipUpstreamConfigKey]; ok {
		cluster = string(externalProviderConfig.Data[KubeVipUpstreamConfigKey])
	}

	if err != nil {
		klog.Errorf("could not load provider config")
		return nil, cluster, err
	}

	klog.Infof("config loaded succesfully. Building client...")

	if _, ok := externalProviderConfig.Data[KubeVipUpstreamClusterKey]; !ok {
		return nil, cluster, errors.New("cluster key not found in secret")
	}

	cluster = string(externalProviderConfig.Data[KubeVipUpstreamClusterKey])
	pc, err := getClientSetAndClusterNameFromSecret(externalProviderConfig)
	if err != nil {
		klog.Errorln("could not build upstream client")
		return nil, cluster, err
	}

	klog.Infoln("provider client successfully initialized")

	return pc, cluster, nil
}

func getClientSetAndClusterNameFromSecret(externalProviderConfig *v1.Secret) (*kubernetes.Clientset, error) {

	var cl *kubernetes.Clientset
	var config clientcmd.ClientConfig

	if _, ok := externalProviderConfig.Data[KubeVipUpstreamConfigKey]; !ok {
		return nil, errors.New("config key not found in secret")
	}

	config, err := clientcmd.NewClientConfigFromBytes(externalProviderConfig.Data[KubeVipUpstreamConfigKey])

	if err != nil {
		klog.Errorf("could not transform provider config")
		return cl, err
	}

	restConfig, err := config.ClientConfig()

	if err != nil {
		klog.Errorf("could not build rest client from provider config")
		return cl, err
	}

	cl, err = kubernetes.NewForConfig(restConfig)

	if err != nil {
		klog.Errorf("could not build kubernetes client set from provider config")
		return cl, err
	}

	return cl, nil
}

func waitForIPAllocation(w watch.Interface) string {

	defer w.Stop()

	for {
		e := <-w.ResultChan()

		svc := e.Object.(*v1.Service)
		klog.Infof("service object: %v", svc)

		if svc.Spec.LoadBalancerIP != "" {
			return svc.Spec.LoadBalancerIP
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
