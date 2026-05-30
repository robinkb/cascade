package operator

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strconv"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/robinkb/cascade/cluster"
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
	}
}

type nodeController struct {
	client client.Client
	self   types.NamespacedName
	node   raft.Node
}

func (r *nodeController) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	if req.NamespacedName == r.self {
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
					Port: new(int32(addr.Port())),
				},
			}
			return nil
		})
	}
	if err != nil {
		return
	}

	es := new(discoveryv1.EndpointSlice)
	err = r.client.Get(ctx, req.NamespacedName, es)
	if err != nil {
		return
	}

	peer, err := peerFromEndpointSlice(es)
	if err != nil {
		return
	}

	if _, ok := r.node.Status().Config.Voters.IDs()[peer.ID]; !ok {
		r.node.Bootstrap(peer)
	}

	return
}

func (r *nodeController) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("cascade-node-controller").
		For(&discoveryv1.EndpointSlice{}).
		WithOptions(controller.TypedOptions[reconcile.Request]{
			NeedLeaderElection: new(false),
		}).
		WatchesRawSource(&RaftEventSource{r.self, r.node}).
		Complete(r)
}

func peerFromEndpointSlice(es *discoveryv1.EndpointSlice) (cluster.Peer, error) {
	var peer cluster.Peer

	id, err := strconv.ParseUint(es.Annotations[AnnotationCascadeNodeID], 10, 64)
	if err != nil {
		return peer, err
	}

	host := es.Endpoints[0].Addresses[0]
	port := *es.Ports[0].Port

	peer.ID = id
	peer.Addr = fmt.Sprintf("%s:%d", host, port)
	return peer, nil
}
