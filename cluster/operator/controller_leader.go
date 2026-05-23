package operator

import (
	"context"
	"fmt"
	"strconv"

	discoveryv1 "k8s.io/api/discovery/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft"
)

func newLeaderController(c client.Client, node raft.Node) *leaderController {
	return &leaderController{
		client: c,
		node:   node,
	}
}

type leaderController struct {
	client client.Client
	node   raft.Node
}

func (r *leaderController) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	es := new(discoveryv1.EndpointSlice)
	err = r.client.Get(ctx, req.NamespacedName, es)
	if err != nil {
		return
	}

	id, _ := strconv.ParseUint(es.Annotations[AnnotationCascadeNodeID], 10, 64)
	host := es.Endpoints[0].Addresses[0]
	port := *es.Ports[0].Port

	err = r.node.AddPeer(cluster.Peer{
		ID:   id,
		Addr: fmt.Sprintf("%s:%d", host, port),
	})
	return
}

func (r *leaderController) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("cascade-leader-controller").
		For(&discoveryv1.EndpointSlice{}).
		Complete(r)
}
