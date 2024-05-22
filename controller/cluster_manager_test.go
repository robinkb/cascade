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
package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/intstr"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applydiscoveryv1 "k8s.io/client-go/applyconfigurations/discovery/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func TestServiceManager(t *testing.T) {
	kubeconfig := "/home/robinkb/.kube/config"
	config, err := buildConfig(kubeconfig)
	if err != nil {
		klog.Fatal(err)
	}
	client := clientset.NewForConfigOrDie(config)
	applyOpts := metav1.ApplyOptions{
		Force:        true,
		FieldManager: "cascade-controller",
	}
	namespace := "kube-system"

	// Start Informer for logging
	selector := labels.NewSelector()
	requirement, err := labels.NewRequirement(discoveryv1.LabelServiceName, selection.Equals, []string{"cascade"})
	if err != nil {
		t.Fatal(err)
	}
	selector.Add(*requirement)

	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		client,
		10*time.Second,
		informers.WithNamespace("kube-system"),
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.LabelSelector = selector.String()
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endpointSliceInformer := informerFactory.Discovery().V1().EndpointSlices()
	endpointSliceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		// Added to the informer, _not_ to the cluster.
		AddFunc: func(obj interface{}) {
			slice := obj.(*discoveryv1.EndpointSlice)
			t.Log("endpointslice added", slice.GetObjectMeta().GetName())
		},
		// Updated in the informer, not necessarily in the cluster.
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldSlice := oldObj.(*discoveryv1.EndpointSlice)
			newSlice := newObj.(*discoveryv1.EndpointSlice)
			if oldSlice.ResourceVersion == newSlice.ResourceVersion {
				t.Log("endpointslice not changed", newSlice.GetObjectMeta().GetName())
			} else {
				t.Log("endpointslice changed", newSlice.GetObjectMeta().GetName())
			}

		},
		// Deleted in the informer, which likely coincides with a delete in the cluster.
		DeleteFunc: func(obj interface{}) {
			slice := obj.(*discoveryv1.EndpointSlice)
			t.Log("endpointslice deleted", slice.GetObjectMeta().GetName())
		},
	})

	informerFactory.Start(ctx.Done())

	// Creating Service
	serviceDef := applycorev1.Service("cascade", namespace).
		WithSpec(applycorev1.ServiceSpec().
			WithClusterIP("None").
			WithPorts(
				applycorev1.ServicePort().
					WithName("nats-cluster").
					WithAppProtocol("TCP").
					WithPort(6222).
					WithTargetPort(intstr.FromString("nats-cluster")),
			),
		)

	serviceObj, err := client.CoreV1().Services(namespace).Apply(ctx, serviceDef, applyOpts)
	if err != nil {
		t.Fatal(err)
	}

	// Creating EndpointSlices
	endpointSlicePrimero := applydiscoveryv1.EndpointSlice("cascade-primero", namespace).
		WithLabels(map[string]string{
			discoveryv1.LabelServiceName: serviceObj.Name,
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
				WithHostname("cascade-primero").
				WithNodeName("primero").
				WithAddresses("192.168.2.11").
				WithTargetRef(
					applycorev1.ObjectReference().
						WithAPIVersion("v1").
						WithKind("Pod").
						WithName("cascade-primero").
						WithNamespace(namespace),
				),
		)

	endpointSliceSegundo := applydiscoveryv1.EndpointSlice("cascade-segundo", namespace).
		WithLabels(map[string]string{
			discoveryv1.LabelServiceName: serviceObj.Name,
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
				WithHostname("cascade-segundo").
				WithNodeName("segundo").
				WithAddresses("192.168.2.12").
				WithTargetRef(
					applycorev1.ObjectReference().
						WithAPIVersion("v1").
						WithKind("Pod").
						WithName("cascade-segundo").
						WithNamespace(namespace),
				),
		)

	endpointSliceTercero := applydiscoveryv1.EndpointSlice("cascade-tercero", namespace).
		WithLabels(map[string]string{
			discoveryv1.LabelServiceName: serviceObj.Name,
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
				WithHostname("cascade-tercero").
				WithNodeName("tercero").
				WithAddresses("192.168.2.13").
				WithTargetRef(
					applycorev1.ObjectReference().
						WithAPIVersion("v1").
						WithKind("Pod").
						WithName("cascade-tercero").
						WithNamespace(namespace),
				),
		)

	t.Log("going to add EndpointSlices")

	_, err = client.DiscoveryV1().EndpointSlices(namespace).Apply(ctx, endpointSlicePrimero, applyOpts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DiscoveryV1().EndpointSlices(namespace).Apply(ctx, endpointSliceSegundo, applyOpts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DiscoveryV1().EndpointSlices(namespace).Apply(ctx, endpointSliceTercero, applyOpts)
	if err != nil {
		t.Fatal(err)
	}

	endpoints, err := client.DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", discoveryv1.LabelServiceName, serviceObj.Name),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range endpoints.Items {
		for _, port := range item.Ports {
			t.Log(*port.Name)
		}
		for _, endpoint := range item.Endpoints {
			t.Log(endpoint.Addresses)
		}
	}

	time.Sleep(60 * time.Second)
}
