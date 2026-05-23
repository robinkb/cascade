package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/alecthomas/kong"
	"go.yaml.in/yaml/v4"

	"github.com/robinkb/cascade/cluster"
	"github.com/robinkb/cascade/cluster/operator"
	"github.com/robinkb/cascade/cluster/raft"
	"github.com/robinkb/cascade/cluster/raft/qwal"
	"github.com/robinkb/cascade/pkg/process"
	"github.com/robinkb/cascade/pkg/server"
	"github.com/robinkb/cascade/registry"
	registryapi "github.com/robinkb/cascade/registry/api/v2"
	"github.com/robinkb/cascade/registry/store"
	storeapi "github.com/robinkb/cascade/registry/store/api"
	"github.com/robinkb/cascade/registry/store/driver/boltdb"
	clusterstore "github.com/robinkb/cascade/registry/store/driver/cluster"
	"github.com/robinkb/cascade/registry/store/driver/fs"

	// Embed tzdata to run from scratch.
	_ "time/tzdata"

	// Embed CA certificates to run from scratch.
	_ "golang.org/x/crypto/x509roots/fallback"
)

var cli struct {
	Config kong.ConfigFlag `help:"Path to a Cascade config file."`

	Port int `help:"Port of the Registry HTTP server." default:"5000"`

	Raft struct {
		ID    uint64   `help:"ID of this Raft node."`
		Host  string   `help:"Host of this Raft node." default:"127.0.0.1"`
		Port  int      `help:"Port of this Raft node." default:"3000"`
		Peers []string `help:"Comma-separated list of Raft peers."`
	} `embed:"" prefix:"raft."`

	Operator struct {
		Namespace string `help:"Kubernetes namespace that the operator runs in." default:"default"`
		PodName   string
	} `embed:"" prefix:"operator."`
}

func main() {
	kong.Parse(&cli,
		kong.DefaultEnvars("cascade"),
		kong.Configuration(yamlResolver, "/etc/cascade/config.yaml"),
	)

	path, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %s", err)
	}

	mgr := process.NewManager()

	metadata, err := boltdb.NewMetadataStore(path)
	if err != nil {
		log.Fatalf("failed to create metadata store backed by boltdb: %s", err)
	}
	blobs := fs.NewBlobStore(path)

	if cli.Raft.ID != 0 || cli.Operator.PodName != "" {
		addr := fmt.Sprintf("%s:%d", cli.Raft.Host, cli.Raft.Port)
		srv := server.New(server.Options{
			Name: "cluster-server",
			Addr: addr,
		})

		peers := make([]cluster.Peer, len(cli.Raft.Peers))
		for i := range cli.Raft.Peers {
			parts := strings.Split(cli.Raft.Peers[i], ":")
			id, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			host := strings.Join(parts[1:3], ":")
			peers[i] = cluster.Peer{
				ID:   id,
				Addr: host,
			}
		}

		db, err := qwal.Open(filepath.Join(path, "raft"), nil)
		if err != nil {
			log.Fatal(err)
		}
		storage, err := raft.NewDiskStorage(db, metadata)
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := storage.Close(); err != nil {
				log.Println("error while closing raft storage:", err)
			}
		}()

		if cli.Raft.ID == 0 {
			id, err := generateOrReadID()
			if err != nil {
				log.Fatal(err)
			}
			cli.Raft.ID = id
		}

		restorer := store.NewRestorer(metadata, blobs)
		node := raft.NewNode(cli.Raft.ID, addr, storage, restorer)
		// Shit, this is needed because the node has to be running.
		// And the node won't be running until the manager starts.
		// And starting the manager is a blocking call.
		// go func() {
		// 	time.Sleep(50 * time.Millisecond)
		// 	node.Bootstrap(peers...)
		// }()

		srv.Handle("/cluster/raft/", node.Handler())
		srv.Handle("/store/", storeapi.New(blobs))
		mgr.Register(srv)
		mgr.Register(node)

		metadata = clusterstore.NewMetadataStore(node, metadata)
		blobs = clusterstore.NewBlobStore(node, blobs)

		operator, err := operator.New(node, cli.Operator.Namespace, cli.Operator.PodName)
		if err != nil {
			log.Fatal(err)
		}
		mgr.Register(operator)
	}

	srv := server.New(server.Options{
		Name:          "oci-api",
		Addr:          fmt.Sprintf("0.0.0.0:%d", cli.Port),
		LoggerEnabled: true,
	})

	service := registry.New(metadata, blobs)
	srv.Handle("/", registryapi.New(service))

	mgr.Register(srv)

	if err := mgr.Run(); err != nil {
		log.Fatal(err)
	}
}

func yamlResolver(r io.Reader) (kong.Resolver, error) {
	values := map[string]any{}
	err := yaml.NewDecoder(r).Decode(values)
	if err != nil {
		return nil, err
	}

	var f kong.ResolverFunc = func(context *kong.Context, parent *kong.Path, flag *kong.Flag) (any, error) {
		name := kebabToCamel(flag.Name)
		fmt.Println(name)
		raw, ok := values[name]
		if ok {
			return raw, nil
		}
		raw = values
		for _, part := range strings.Split(name, ".") {
			if values, ok := raw.(map[string]any); ok {
				raw, ok = values[part]
				if !ok {
					return nil, nil
				}
			} else {
				return nil, nil
			}
		}
		return raw, nil
	}

	return f, nil
}

func kebabToCamel(s string) string {
	var b strings.Builder
	upperNext := false

	for _, r := range s {
		if r == '-' {
			upperNext = true
			continue
		}

		if upperNext {
			r = unicode.ToUpper(r)
			upperNext = false
		}

		b.WriteRune(r)
	}

	return b.String()
}

func generateOrReadID() (uint64, error) {
	buf := make([]byte, 8)

	file, err := os.Open("raft/node-id")
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatal(err)
		}

		id := rand.Uint64()
		binary.LittleEndian.PutUint64(buf, id)
		err = os.WriteFile("raft/node-id", buf, os.ModeAppend)
		return id, err
	}

	_, err = io.ReadFull(file, buf)
	if err != nil {
		return 0, err
	}
	id := binary.LittleEndian.Uint64(buf)
	return id, nil
}
