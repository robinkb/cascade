package e2e

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/pkg/process"
	"github.com/robinkb/cascade/pkg/server"
	"github.com/robinkb/cascade/registry"
	registryapi "github.com/robinkb/cascade/registry/api/v2"
	"github.com/robinkb/cascade/registry/store"
	storeapi "github.com/robinkb/cascade/registry/store/api"
	clusterstore "github.com/robinkb/cascade/registry/store/driver/cluster"
	"github.com/robinkb/cascade/registry/store/driver/inmemory"

	. "github.com/robinkb/cascade/testing" // nolint: staticcheck
)

type TestNodeOptions struct {
	DBOptions *qwal.Options
}

func NewTestNode(t testing.TB, id uint64, opts *TestNodeOptions) TestNode {
	if opts == nil {
		opts = &TestNodeOptions{}
	}

	dir := t.TempDir()
	mgr := process.NewManager()
	metadata := inmemory.NewMetadataStore()
	blobs := inmemory.NewBlobStore()

	raftHostPort := fmt.Sprintf("0.0.0.0:%d", 3000+id)
	raftServer := server.New(server.Options{
		Name: "raft-server",
		Addr: raftHostPort,
	})
	db, err := qwal.Open(dir, opts.DBOptions)
	AssertNoError(t, err).Require()
	storage, err := raft.NewDiskStorage(dir, db, metadata)
	AssertNoError(t, err).Require()

	restorer := store.NewRestorer(metadata, blobs)
	node := raft.NewNode(id, raftHostPort, storage, restorer)
	raftServer.Handle("/cluster/raft/", node.Handler())
	raftServer.Handle("/store/", storeapi.New(blobs))
	mgr.Register(raftServer)
	mgr.Register(node)

	metadata = clusterstore.NewMetadataStore(node, metadata)
	blobs = clusterstore.NewBlobStore(node, blobs)

	registryServer := server.New(server.Options{
		Name: "registry-server",
		Addr: fmt.Sprintf("0.0.0.0:%d", 5000+id),
	})
	service := registry.New(metadata, blobs)
	registryapi := registryapi.New(service)
	registryServer.Handle("/", registryapi)
	mgr.Register(registryServer)

	return TestNode{
		Manager: mgr,

		Metadata:       metadata,
		Blobs:          blobs,
		Registry:       service,
		RegistryAPI:    registryapi,
		RegistryServer: registryServer,

		RaftServer:  raftServer,
		DB:          db,
		DiskStorage: storage,
		Node:        node,
	}
}

type TestNode struct {
	Manager *process.Manager

	Metadata       store.Metadata
	Blobs          store.Blobs
	Registry       registry.Service
	RegistryAPI    http.Handler
	RegistryServer *server.Server

	RaftServer  *server.Server
	DB          qwal.DB
	DiskStorage *raft.DiskStorage
	Node        raft.Node
}

// Run starts a Node on a Go routine, and blocks until it is started.
// The Node is shut down at the end of the test.
func Run(t testing.TB, n TestNode) {
	t.Cleanup(func() {
		err := n.Manager.Shutdown()
		AssertNoError(t, err)
	})

	go func(t testing.TB) {
		t.Helper()
		err := n.Manager.Run()
		AssertNoError(t, err)
	}(t)

	for {
		// Effectively waits for the Raft node to start.
		// Nodes are not allowed to have ID 0, which is the zero value
		// in the status. If it's not 0, that means that the node has started.
		if n.Node.Status().ID != 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
		n.Node.Tick()
	}
}
