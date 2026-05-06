# Cascade

An OCI Registry that gets out of your way.

Cascade is an implementation of the CNCF Distribution spec.
Use it to serve OCI artifacts like container images and Helm charts.

- **Standards Compliant**: Use your existing tools without making any changes.
- **Built-in Replication**: Form a cluster to replicate all data in the registry to each node.
  Clients can push blobs to and pull blobs from any node in the cluster.
- **Deduplicated Storage**: Blobs are re-used between repositories, saving valuable space.
- **Online Garbage Collection**: Deleting a tag or manifest automatically cleans up all unused blobs.
- **Efficient**: Serve blobs with a minimal memory footprint, even in cluster mode.

> [!IMPORTANT]
> This is experimental software.
> You should not use it in production.

## Installation

You can run Cascade with the provided container image:

```sh
docker pull ghcr.io/robinkb/cascade:latest
```

You can also install it from source:

```sh
go install github.com/robinkb/cascade/cmd/cascade-registry@main
```

Versioned packages are not yet available.

## Quick Start

You can run Cascade as a standalone registry.

```bash
cascade-registry -p 5000
```

You can also run Cascade in a cluster.

```bash
docker-compose -f compose/compose.yaml up
```

This will pull the images from GitHub Container Registry.
If you want to build the images from source, you can add the `--build` flag.

```bash
docker-compose -f compose/compose.yaml up --build
```

The registry will be available on port 5000, fronted by a loadbalancer.
Port 8000 will serve [Quiq/registry-ui](https://github.com/Quiq/registry-ui) connected to the Cascade cluster.

## Limitations

Cascade is still in early stages.
Some basic expected features are not yet supported, like HTTPS connections.

Breaking changes are expected to happen in various parts of Cascade.
Certain breaking changes may require data migrations instead of in-place upgrades.
In that case, you can use tools like `skopeo sync` to copy all data between registries.

## Developing

There are a few generated mocks in the `testing` package.
These mocks are generated with [Mockery](https://vektra.github.io/mockery/latest/).
To update the mocks, run `go tool mockery`.
The Mockery configuration in [`.mockery.yaml`](/.mockery.yaml) has lots of comments.
