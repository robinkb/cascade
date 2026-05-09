package controller

import (
	"context"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/robinkb/cascade/cluster/raft"
)

const (
	AnnotationCascadeNodeID string = "registry.cascade.redbreast.systems/node-id"
)

func newNodeReconciler(c client.Client) *nodeReconciler {
	return &nodeReconciler{
		client: c,
		events: make(chan event.GenericEvent),
	}
}

type nodeReconciler struct {
	client client.Client
	node   raft.Node
	events chan event.GenericEvent
}

func (r *nodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	err = r.client.Create(ctx, &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            req.Name,
			Namespace:       req.Namespace,
			Annotations:     make(map[string]string),
			OwnerReferences: make([]metav1.OwnerReference, 0),
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints:   make([]discoveryv1.Endpoint, 0),
		Ports:       make([]discoveryv1.EndpointPort, 0),
	})

	return
}

// Enqueue manually triggers the reconciler.
func (r *nodeReconciler) Enqueue() {
	r.events <- event.GenericEvent{
		Object: &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "foo",
				Namespace: "kube-system",
			},
		},
	}
}

func (r *nodeReconciler) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("cascade-node-controller").
		WithOptions(controller.TypedOptions[reconcile.Request]{
			NeedLeaderElection: ptr.To(false),
		}).
		WatchesRawSource(source.Channel(
			r.events, &handler.EnqueueRequestForObject{},
		)).
		Complete(r)
}
