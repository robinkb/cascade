package controller

import (
	"fmt"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	. "github.com/robinkb/cascade/testing"
	"github.com/robinkb/cascade/testing/raft/mock"
)

func testReconcilerNode(t *testing.T, config *rest.Config) {
	node := mock.NewNode(t)
	client, err := client.New(config, client.Options{})
	AssertNoError(t, err).Require()

	r := nodeReconciler{client: client, node: node}
	_, err = r.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "kube-system", Name: "bar"}})
	AssertNoError(t, err)

	var es discoveryv1.EndpointSlice
	err = client.Get(t.Context(), types.NamespacedName{Namespace: "kube-system", Name: "bar"}, &es)
	AssertNoError(t, err)
	data, err := yaml.Marshal(es)
	AssertNoError(t, err)

	fmt.Printf("%s", data)
}
