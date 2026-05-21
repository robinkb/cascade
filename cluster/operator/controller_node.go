package operator

import (
	"context"
	"errors"
	"net/netip"
	"strconv"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/robinkb/cascade/cluster/raft"
)

const (
	AnnotationCascadeNodeID string = "registry.cascade.redbreast.systems/node-id"
)

var (
	ErrUnexpectedEndpointSlice = errors.New("received unexpected endpointslice")
)

func newNodeController(c client.Client, n raft.Node, name types.NamespacedName) *nodeController {
	return &nodeController{
		client: c,
		node:   n,
		self:   name,
		events: make(chan event.GenericEvent),
	}
}

type nodeController struct {
	client client.Client
	self   types.NamespacedName
	node   raft.Node
	events chan event.GenericEvent
}

func (r *nodeController) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	logger := log.FromContext(ctx).WithValues("endpointslice", req)

	if req.NamespacedName != r.self {
		err = ErrUnexpectedEndpointSlice
		logger.Error(ErrUnexpectedEndpointSlice, "")
		return
	}

	es := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: req.Namespace, Name: req.Name}}
	peer := r.node.AsPeer()
	addr := netip.MustParseAddrPort(peer.Addr)

	_, err = controllerutil.CreateOrPatch(ctx, r.client, es, func() error {
		if es.Annotations == nil {
			es.Annotations = make(map[string]string)
		}
		es.Annotations[AnnotationCascadeNodeID] = strconv.FormatUint(peer.ID, 10)
		if es.Labels == nil {
			es.Labels = make(map[string]string)
		}
		es.Labels = labels.Merge(es.Labels, commonLabels)
		es.AddressType = discoveryv1.AddressTypeIPv4
		es.Endpoints = []discoveryv1.Endpoint{
			discoveryv1.Endpoint{
				Addresses: []string{addr.Addr().String()},
			},
		}
		es.Ports = []discoveryv1.EndpointPort{
			discoveryv1.EndpointPort{
				Port: ptr.To(int32(addr.Port())),
			},
		}
		return nil
	})

	return
}

// Enqueue manually triggers the reconciler.
func (r *nodeController) Enqueue() {
	r.events <- event.GenericEvent{
		Object: &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.self.Name,
				Namespace: r.self.Namespace,
			},
		},
	}
}

func (r *nodeController) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("cascade-node-controller").
		For(&discoveryv1.EndpointSlice{}).
		WithOptions(controller.TypedOptions[reconcile.Request]{
			NeedLeaderElection: ptr.To(false),
		}).
		WithEventFilter(predicate.NewPredicateFuncs(func(object client.Object) bool {
			return object.GetNamespace() == r.self.Namespace && object.GetName() == r.self.Name
		})).
		WatchesRawSource(source.Channel(
			r.events, &handler.EnqueueRequestForObject{},
		)).
		Complete(r)
}
