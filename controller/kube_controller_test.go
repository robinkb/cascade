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
	"math/rand"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
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

	namespace := createTestingNamespace(t, client)
	hosts := []struct {
		name string
		host string
		port int32
	}{
		{
			name: "s1",
			host: "192.168.0.10",
			port: rand.Int31(),
		},
		{
			name: "s2",
			host: "192.168.0.11",
			port: rand.Int31(),
		},
		{
			name: "s3",
			host: "192.168.0.12",
			port: rand.Int31(),
		},
	}

	sds := make([]ServiceDiscovery, 0)
	for _, host := range hosts {
		sd, err := NewKubernetesDiscoveryClient(ctx, client, host.name, namespace)
		if err != nil {
			t.Fatal(err)
		}

		sd.Start(ctx.Done())
		sds = append(sds, sd)
	}

	for _, sd := range sds {
		routes, err := sd.Routes()
		if err != nil {
			t.Fatal(err)
		}

		// TODO: Better check here
		if len(routes) != len(hosts) {
			t.Errorf("unexpected amount of routes returned; found %d, want %d", len(routes), len(hosts))
		}
	}
}

func createTestingNamespace(t *testing.T, client kubernetes.Interface) string {
	t.Helper()
	namespace := string(uuid.NewUUID())

	_, err := client.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		client.CoreV1().Namespaces().Delete(context.Background(), namespace, metav1.DeleteOptions{})
	})

	return namespace
}
