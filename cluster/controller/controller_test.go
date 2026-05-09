package controller

import (
	"fmt"
	"testing"

	. "github.com/robinkb/cascade/testing"
	"github.com/robinkb/cascade/testing/raft/mock"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"
)

func TestController(t *testing.T) {
	path, err := envtest.SetupEnvtestDefaultBinaryAssetsDirectory()
	AssertNoError(t, err).Require()

	env := envtest.Environment{
		// AttachControlPlaneOutput: true,
		DownloadBinaryAssets:  true,
		BinaryAssetsDirectory: path,
	}
	config, err := env.Start()
	AssertNoError(t, err).Require()
	t.Cleanup(func() {
		err = env.Stop()
		AssertNoError(t, err).Require()
	})

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
