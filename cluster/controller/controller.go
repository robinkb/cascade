package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/raft"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	discoveryv1ac "k8s.io/client-go/applyconfigurations/discovery/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	AnnotationCascadeNodeID string = "registry.cascade.redbreast.systems/node-id"
)

var bruh = predicate.Funcs{
	CreateFunc:  func(tce event.TypedCreateEvent[client.Object]) bool { return false },
	UpdateFunc:  func(tue event.TypedUpdateEvent[client.Object]) bool { return false },
	DeleteFunc:  func(tde event.TypedDeleteEvent[client.Object]) bool { return false },
	GenericFunc: func(tge event.TypedGenericEvent[client.Object]) bool { return false },
}

func New(node raft.Node) (*Manager, error) {
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Cache: cache.Options{}, // TODO: Configure cache to only watch current namespace

	})
	if err != nil {
		return nil, err
	}

	lr := leaderReconciler{node: node, client: mgr.GetClient()}
	if err := lr.SetupWithManager(mgr); err != nil {
		return nil, err
	}

	nr := nodeReconciler{node: node, client: mgr.GetClient()}
	if err := nr.SetupWithManager(mgr); err != nil {
		return nil, err
	}

	return &Manager{
		mgr: mgr,
	}, nil
}

type Manager struct {
	mgr      manager.Manager
	shutdown context.CancelFunc
}

func (m *Manager) Name() string {
	return "controller-manager"
}

func (m *Manager) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.shutdown = cancel

	return m.mgr.Start(ctx)
}

func (m *Manager) Shutdown() error {
	m.shutdown()
	return nil
}

type leaderReconciler struct {
	client client.Client
	node   raft.Node
}

func (r *leaderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	es := new(discoveryv1.EndpointSlice)
	err = r.client.Get(ctx, req.NamespacedName, es)
	if err != nil {
		return
	}

	id, _ := strconv.ParseUint(es.Annotations[AnnotationCascadeNodeID], 10, 64)
	host := es.Endpoints[0].Addresses[0]
	port := *es.Ports[0].Port

	r.node.AddPeer(cluster.Peer{
		ID:   id,
		Addr: fmt.Sprintf("%s:%d", host, port),
	})
	return
}

func (r *leaderReconciler) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("cascade-leader-controller").
		// TODO: Option to only reconcile in its own namespace.
		// TODO: Predicate to only reconcile EndpointSlice matching label selector.
		For(&discoveryv1.EndpointSlice{}).
		Complete(r)
}

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

	esac := discoveryv1ac.EndpointSlice(req.Name, req.Namespace).
		WithAnnotations(map[string]string{
			AnnotationCascadeNodeID: strconv.FormatUint(uint64(99999), 10),
		}).
		WithEndpoints(discoveryv1ac.Endpoint().
			WithAddresses("1.1.1.1"),
		).
		WithPorts(discoveryv1ac.EndpointPort().
			WithPort(42000),
		)

	err = r.client.Apply(ctx, esac, &client.ApplyOptions{
		Force:        ptr.To(true),
		FieldManager: "cascade-node-controller",
	})

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
