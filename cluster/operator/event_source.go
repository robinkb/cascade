package operator

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/robinkb/cascade/cluster/raft"
)

// RaftEventSource implements ["sigs.k8s.io/controller-runtime/pkg/source".Source],
// queueing up a [reconcile.Request] based on received Raft events.
type RaftEventSource struct {
	self types.NamespacedName
	node raft.Node
}

// Start implements [source.Source.Start].
func (s *RaftEventSource) Start(ctx context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	// Populate the queue with an initial item to trigger a reconcile at startup.
	queue.Add(reconcile.Request{NamespacedName: s.self})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-s.node.Emit(ctx, "node-controller"):
				switch e.Reason {
				case raft.ReasonStateChanged:
					queue.Add(reconcile.Request{
						NamespacedName: s.self,
					})
				}
			}
		}
	}()

	return nil
}
