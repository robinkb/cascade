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
	"os"
	"path"
	"testing"

	"github.com/robinkb/cascade/controller"
	"github.com/robinkb/cascade/controller/testsuites"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func TestKubernetesServiceDiscovery(t *testing.T) {
	homedir, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	config, err := clientcmd.BuildConfigFromFlags("", path.Join(homedir, ".kube/config"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	namespace := createTestingNamespace(t, client)

	serviceDiscoveryConstructor := func(clusterRoute *controller.ClusterRoute) (controller.ServiceDiscovery, error) {
		return NewServiceDiscovery(client, namespace, clusterRoute)
	}

	testsuites.ServiceDiscovery(t, serviceDiscoveryConstructor)
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
