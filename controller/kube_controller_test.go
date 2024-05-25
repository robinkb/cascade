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
	"testing"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func TestKubernetesDiscoveryClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := clientcmd.BuildConfigFromFlags("", "/home/robinkb/.kube/config")
	if err != nil {
		t.Fatal(err)
	}
	client, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	dc1, err := NewKubernetesDiscoveryClient(ctx, client, "s1", "kube-system")
	if err != nil {
		t.Fatal(err)
	}

	dc1.Start(ctx.Done())

	dc2, err := NewKubernetesDiscoveryClient(ctx, client, "s2", "kube-system")
	if err != nil {
		t.Fatal(err)
	}

	dc2.Start(ctx.Done())

	dc3, err := NewKubernetesDiscoveryClient(ctx, client, "s3", "kube-system")
	if err != nil {
		t.Fatal(err)
	}

	dc3.Start(ctx.Done())

	routes, err := dc1.Routes()
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		t.Log(route.String())
	}

	routes, err = dc2.Routes()
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		t.Log(route.String())
	}

	routes, err = dc3.Routes()
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		t.Log(route.String())
	}

	// TODO: Remove endpointSlices, or better yet, create and remove an ephemeral namespace.
}
