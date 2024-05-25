package kubernetes

import (
	"github.com/robinkb/cascade/controller"
	"k8s.io/client-go/kubernetes"
)

func NewController(client kubernetes.Interface, namespace string, clusterRoute *controller.ClusterRoute) (controller.Controller, error) {
	sd, err := NewServiceDiscovery(client, namespace, clusterRoute)
	if err != nil {
		return nil, err
	}

	return controller.NewController(sd), nil
}
