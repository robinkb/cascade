package controller

import (
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	. "github.com/robinkb/cascade/testing"
)

func testReconcilerNode(t *testing.T, c client.Client) {
	t.Run("creates its EndpointSlice when it does not exist", func(t *testing.T) {
		req := request(randomNamespace(t, c), "bar")

		r := nodeReconciler{client: c}
		_, err := r.Reconcile(t.Context(), req)
		AssertNoError(t, err)

		var es discoveryv1.EndpointSlice
		err = c.Get(t.Context(), req.NamespacedName, &es)
		AssertNoError(t, err)
	})
}
