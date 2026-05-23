package operator

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/robinkb/cascade/cluster/raft"
)

func init() {
	log.SetLogger(zap.New())
}

func New(node raft.Node, namespace, name string) (*Operator, error) {
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Cache:                   cache.Options{}, // TODO: Configure cache to only watch current namespace
		LeaderElection:          true,
		LeaderElectionID:        "cascade-registry-controller",
		LeaderElectionNamespace: namespace,
		Metrics: server.Options{
			BindAddress: "0",
		},
	})
	if err != nil {
		return nil, err
	}

	tname := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	// lr := newLeaderReconciler(mgr.GetClient())
	// if err := lr.SetupWithManager(mgr); err != nil {
	// 	return nil, err
	// }

	nr := newNodeController(mgr.GetClient(), node, tname)
	if err := nr.SetupWithManager(mgr); err != nil {
		return nil, err
	}

	return &Operator{
		mgr: mgr,
		nr:  nr,
	}, nil
}

type Operator struct {
	mgr      manager.Manager
	shutdown context.CancelFunc
	done     chan struct{}

	nr *nodeController
}

func (o *Operator) Name() string {
	return "operator"
}

func (o *Operator) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	o.shutdown = cancel

	go o.nr.Enqueue()

	return o.mgr.Start(ctx)
}

func (o *Operator) Shutdown() error {
	o.shutdown()
	return nil
}
