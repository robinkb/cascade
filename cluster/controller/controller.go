package controller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/robinkb/cascade/cluster/raft"
)

func New(node raft.Node) (*Controller, error) {
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

	return &Controller{
		mgr: mgr,
	}, nil
}

type Controller struct {
	mgr      manager.Manager
	shutdown context.CancelFunc
}

func (m *Controller) Name() string {
	return "controller-manager"
}

func (m *Controller) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.shutdown = cancel

	return m.mgr.Start(ctx)
}

func (m *Controller) Shutdown() error {
	m.shutdown()
	return nil
}
