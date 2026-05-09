package controller

import (
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	. "github.com/robinkb/cascade/testing"
)

func testReconcilerNode(t *testing.T, c client.Client) {
	t.Parallel()

	t.Run("creates its EndpointSlice when it does not exist", func(t *testing.T) {
		ctx, req := t.Context(), request(randomNamespace(t, c), "foo")

		var es discoveryv1.EndpointSlice
		err := c.Get(ctx, req.NamespacedName, &es)
		Assert(t, apierrors.IsNotFound(err))

		r := newNodeReconciler(c)
		result, err := r.Reconcile(ctx, req)
		Assert(t, result.IsZero())
		AssertNoError(t, err)

		err = c.Get(ctx, req.NamespacedName, &es)
		AssertNoError(t, err)
	})

	t.Run("created EndPointSlice contains expected fields", func(t *testing.T) {
		// TODO: Fill in
	})
}
