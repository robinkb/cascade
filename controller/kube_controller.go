package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

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

func NewKubernetesDiscoveryClient(ctx context.Context, client kubernetes.Interface, serverName, namespace string) (*kubernetesDiscoveryClient, error) {
	dc := &kubernetesDiscoveryClient{
		client:    client,
		namespace: namespace,
		applyOpts: metav1.ApplyOptions{
			Force:        true,
			FieldManager: "cascade-controller",
		},
	}

	selector := labels.NewSelector()
	requirement, err := labels.NewRequirement("app.kubernetes.io/name", selection.Equals, []string{"cascade"})
	if err != nil {
		return nil, err
	}
	selector = selector.Add(*requirement)

	// Feels like I should not be getting the IP here.
	ipAddr, err := getLocalIP()
	if err != nil {
		return nil, err
	}

	// TODO: Should use the cluster name in the endpoint slice name, and probably in the labels too.
	endpointSlice := applydiscoveryv1.EndpointSlice(fmt.Sprintf("cascade-%s", serverName), namespace).
		WithLabels(map[string]string{
			"app.kubernetes.io/name": "cascade",
		}).
		WithAddressType(discoveryv1.AddressTypeIPv4).
		WithPorts(
			applydiscoveryv1.EndpointPort().
				WithName("nats-cluster").
				WithProtocol(corev1.ProtocolTCP).
				WithPort(6222),
		).
		WithEndpoints(
			applydiscoveryv1.Endpoint().
				WithHostname(serverName).
				WithNodeName(serverName).
				WithAddresses(ipAddr).
				WithTargetRef(
					applycorev1.ObjectReference().
						WithAPIVersion("v1").
						WithKind("Pod").
						WithName(serverName).
						WithNamespace(namespace),
				),
		)
	dc.endpointSlice = endpointSlice

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		30*time.Second,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = selector.String()
		}),
	)
	dc.informerFactory = informerFactory

	informer := informerFactory.Discovery().V1().EndpointSlices()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    dc.add,
		UpdateFunc: dc.update,
		DeleteFunc: dc.delete,
	})
	dc.informer = informer

	return dc, nil
}

type kubernetesDiscoveryClient struct {
	client          kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	informer        v1.EndpointSliceInformer
	endpointSlice   *applydiscoveryv1.EndpointSliceApplyConfiguration
	namespace       string
	applyOpts       metav1.ApplyOptions
	refresh         chan struct{}
}

func (dc *kubernetesDiscoveryClient) Start(stopCh <-chan struct{}) {
	dc.informerFactory.Start(stopCh)
	dc.reconcile()
	cache.WaitForCacheSync(stopCh, dc.informer.Informer().HasSynced)
}

func (dc *kubernetesDiscoveryClient) Routes() ([]*url.URL, error) {
	slices, err := dc.informer.Lister().EndpointSlices(dc.namespace).List(labels.Everything())
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

func (dc *kubernetesDiscoveryClient) Refresh() <-chan struct{} {
	return dc.refresh
}

func (dc *kubernetesDiscoveryClient) reconcile() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := dc.client.DiscoveryV1().EndpointSlices(dc.namespace).Apply(ctx, dc.endpointSlice, dc.applyOpts)
	if err != nil {
		log.Print(err)
	}

	select {
	case dc.refresh <- struct{}{}:
	case <-time.After(1 * time.Millisecond):
	}
}

func (dc *kubernetesDiscoveryClient) add(obj any) {}
func (dc *kubernetesDiscoveryClient) update(oldObj, newObj any) {
	newSlice := newObj.(*discoveryv1.EndpointSlice)
	if newSlice.GetObjectMeta().GetName() != *dc.endpointSlice.Name {
		return
	}

	oldSlice := oldObj.(*discoveryv1.EndpointSlice)
	if oldSlice.ResourceVersion == newSlice.ResourceVersion {
		return
	}

	dc.reconcile()
}
func (dc *kubernetesDiscoveryClient) delete(obj any) {
	slice := obj.(*discoveryv1.EndpointSlice)
	if slice.ObjectMeta.Name != *dc.endpointSlice.Name {
		return
	}

	dc.reconcile()
}
