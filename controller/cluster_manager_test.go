package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	applydiscoveryv1 "k8s.io/client-go/applyconfigurations/discovery/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	ctx := context.Background()
	applyOpts := metav1.ApplyOptions{
		Force:        true,
		FieldManager: "cascade-controller",
	}
	namespace := "kube-system"

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
}
