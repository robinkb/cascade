package operator

import (
	"fmt"
	"strconv"
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/robinkb/cascade/cluster"
	. "github.com/robinkb/cascade/testing"
	"github.com/robinkb/cascade/testing/raft/mock"
)

func testControllerNode(t *testing.T, c client.Client) {
	t.Parallel()

	t.Run("creates its EndpointSlice when it does not exist", func(t *testing.T) {
		ctx, req := t.Context(), request(randomNamespace(t, c), "foo")
		host := "192.168.1.10"
		port := 3000
		peer := cluster.Peer{ID: 123, Addr: fmt.Sprintf("%s:%d", host, port)}
		want := discoveryv1.EndpointSlice{
			ObjectMeta: v1.ObjectMeta{
				Name:      req.Name,
				Namespace: req.Namespace,
				Annotations: map[string]string{
					AnnotationCascadeNodeID: strconv.Itoa(int(peer.ID)),
				},
				Labels: commonLabels,
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{
				discoveryv1.Endpoint{
					Addresses: []string{host},
				},
			},
			Ports: []discoveryv1.EndpointPort{
				discoveryv1.EndpointPort{
					Port: ptr.To(int32(port)),
				},
			},
		}
		node := mock.NewNode(t)
		node.EXPECT().
			AsPeer().
			Return(peer)

		es := new(discoveryv1.EndpointSlice)
		err := c.Get(ctx, req.NamespacedName, es)
		Assert(t, apierrors.IsNotFound(err))

		r := newNodeController(c, node, req.Namespace)
		result, err := r.Reconcile(ctx, req)
		Assert(t, result.IsZero())
		AssertNoError(t, err)

		err = c.Get(ctx, req.NamespacedName, es)
		AssertNoError(t, err)
		AssertMapsEqual(t, es.Annotations, want.Annotations)
		AssertMapsEqual(t, es.Labels, want.Labels)
		AssertEqual(t, es.AddressType, want.AddressType)
		AssertDeepEqual(t, es.Endpoints, want.Endpoints)
		Assert(t, len(es.Ports) == 1).Require()
		Assert(t, ptr.Equal(es.Ports[0].Port, want.Ports[0].Port))
	})

	t.Run("created EndPointSlice contains expected fields", func(t *testing.T) {
		// TODO: Fill in
	})
}
