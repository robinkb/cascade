/*
Copyright © 2024 Robin Ketelbuters

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
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/robinkb/cascade/controller"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applydiscoveryv1 "k8s.io/client-go/applyconfigurations/discovery/v1"
	"k8s.io/client-go/informers"
	v1 "k8s.io/client-go/informers/discovery/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type ServiceDiscoveryOptions struct {
	Namespace string
}

func NewServiceDiscovery(client kubernetes.Interface, namespace, clusterName string) (controller.ServiceDiscovery, error) {
	sd := &serviceDiscovery{
		client:      client,
		clusterName: clusterName,
		namespace:   namespace,
		applyOpts: metav1.ApplyOptions{
			Force:        true,
			FieldManager: "cascade-controller",
		},
		refresh: make(chan struct{}, 1),
	}

	nameLabel, err := labels.NewRequirement("app.kubernetes.io/name", selection.Equals, []string{"cascade"})
	if err != nil {
		return nil, err
	}
	instanceLabel, err := labels.NewRequirement("app.kubernetes.io/instance", selection.Equals, []string{clusterName})
	if err != nil {
		return nil, err
	}
	selector := labels.NewSelector().
		Add(*nameLabel).
		Add(*instanceLabel)

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		30*time.Second,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = selector.String()
		}),
	)
	sd.informerFactory = informerFactory

	informer := informerFactory.Discovery().V1().EndpointSlices()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sd.add,
		UpdateFunc: sd.update,
		DeleteFunc: sd.delete,
	})
	sd.informer = informer

	return sd, nil
}

type serviceDiscovery struct {
	clusterName     string
	client          kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	informer        v1.EndpointSliceInformer
	endpointSlice   *applydiscoveryv1.EndpointSliceApplyConfiguration
	namespace       string
	applyOpts       metav1.ApplyOptions
	refresh         chan struct{}
}

func (sd *serviceDiscovery) Start(stopCh <-chan struct{}) {
	sd.informerFactory.Start(stopCh)
	sd.reconcile()
	cache.WaitForCacheSync(stopCh, sd.informer.Informer().HasSynced)
}

func (sd *serviceDiscovery) Register(clusterRoute *controller.ClusterRoute) {
	endpointSlice := applydiscoveryv1.EndpointSlice(fmt.Sprintf("%s-%s", sd.clusterName, clusterRoute.ServerName), sd.namespace).
		WithLabels(map[string]string{
			"app.kubernetes.io/name":     "cascade",
			"app.kubernetes.io/instance": sd.clusterName,
		}).
		WithAddressType(discoveryv1.AddressTypeIPv4).
		WithPorts(
			applydiscoveryv1.EndpointPort().
				WithName("nats-cluster").
				WithProtocol(corev1.ProtocolTCP).
				WithPort(clusterRoute.Port),
		).
		WithEndpoints(
			applydiscoveryv1.Endpoint().
				WithHostname(clusterRoute.ServerName).
				WithNodeName(clusterRoute.ServerName).
				WithAddresses(clusterRoute.IPAddr).
				WithTargetRef(
					applycorev1.ObjectReference().
						WithAPIVersion("v1").
						WithKind("Pod").
						WithName(clusterRoute.ServerName).
						WithNamespace(sd.namespace),
				),
		)

	sd.endpointSlice = endpointSlice
	sd.reconcile()
}

func (sd *serviceDiscovery) Routes() ([]*url.URL, error) {
	slices, err := sd.informer.Lister().EndpointSlices(sd.namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	urls := make([]*url.URL, 0)
	for _, slice := range slices {
		var portNumber int32
		for _, port := range slice.Ports {
			if port.Name != nil && *port.Name == "nats-cluster" && port.Port != nil {
				portNumber = *port.Port
				break
			}
		}

		if portNumber == 0 {
			return nil, errors.New("no port number found")
		}

		for _, endpoint := range slice.Endpoints {
			for _, address := range endpoint.Addresses {
				urls = append(urls, &url.URL{
					Host:   fmt.Sprintf("%s:%d", address, portNumber),
					Scheme: "nats",
				})
			}
		}
	}

	return urls, nil
}

func (sd *serviceDiscovery) Refresh() <-chan struct{} {
	return sd.refresh
}

func (sd *serviceDiscovery) sendRefresh() {
	select {
	case sd.refresh <- struct{}{}:
	case <-time.After(1 * time.Millisecond):
	}
}

func (sd *serviceDiscovery) reconcile() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := sd.client.DiscoveryV1().EndpointSlices(sd.namespace).Apply(ctx, sd.endpointSlice, sd.applyOpts)
	if err != nil {
		log.Print(err)
	}

	sd.sendRefresh()
}

func (sd *serviceDiscovery) add(obj any) {
	sd.sendRefresh()
}

func (sd *serviceDiscovery) update(oldObj, newObj any) {
	newSlice := newObj.(*discoveryv1.EndpointSlice)
	if newSlice.GetObjectMeta().GetName() != *sd.endpointSlice.Name {
		return
	}

	oldSlice := oldObj.(*discoveryv1.EndpointSlice)
	if oldSlice.ResourceVersion == newSlice.ResourceVersion {
		return
	}

	sd.reconcile()
}

func (sd *serviceDiscovery) delete(obj any) {
	slice := obj.(*discoveryv1.EndpointSlice)
	if slice.ObjectMeta.Name != *sd.endpointSlice.Name {
		return
	}

	sd.reconcile()
}
