package controller

import (
	"context"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

type nodeReconciler struct {
	client client.Client
	node   raft.Node
}

func (r *nodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	es := new(discoveryv1.EndpointSlice)
	err = r.client.Get(ctx, req.NamespacedName, es)
	if err != nil {
		if apierrors.IsNotFound(err) {
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
			if err != nil {
				return
			}
		}
	}

	return
}

func (r *nodeReconciler) SetupWithManager(mgr manager.Manager) error {
	events := make(chan event.GenericEvent)

	go func() {
		ticker := time.Tick(10 * time.Minute)
		for {
			events <- event.GenericEvent{
				Object: &discoveryv1.EndpointSlice{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "kube-system",
					},
				},
			}
			<-ticker
		}
	}()

	return ctrl.NewControllerManagedBy(mgr).
		Named("cascade-node-controller").
		WithOptions(controller.TypedOptions[reconcile.Request]{
			NeedLeaderElection: ptr.To(false),
		}).
		// TODO: Am I being difficult here? Should I just create the object with a Kubernetes client
		// before the manager starts? That seems much easier... And simpler. Just keep it simple.
		// And then reconcile on CUD events to restore it to expected state.
		WatchesRawSource(source.Channel(
			events, &handler.EnqueueRequestForObject{},
		)).
		Complete(r)
}
