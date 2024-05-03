# Cascade

Cascade is an OCI Registry built with [CNCF Distribution](https://distribution.github.io/distribution) on top of [NATS](https://nats.io).

The purpose of Cascade is to be a **replicated** OCI Registry that is easy to run.
As of writing, all other OCI registries rely on object storage for replication.
Not ideal for environments that don't run in the cloud, or have limited resources.
Self-hosted S3 implementations also tend to be quite difficult to operate, or are not lightweight.
Cascade aims to solve this by using [NATS Object Store](https://docs.nats.io/nats-concepts/jetstream/obj_store) as its storage layer, and eventually by having the ability to operate NATS itself through a custom controller.

> [!IMPORTANT]
> This is experimental software.
> You should not use it in production.


## Installation

There are binaries available on the [Releases](https://github.com/robinkb/cascade/releases) page.

Container images are available as well:

```
podman pull ghcr.io/robinkb/cascade:<version>
```

Or you can compile Cascade yourself with `go install`:

```
go install github.com/robinkb/cascade/cmd/cascade@latest
```


## Quick Start

You can quickly run Cascade with the NATS and Cascade binaries:


```shell
# Run the NATS server.
nats-server --jetstream
# Run Cascade with the example configuration.
cascade serve cmd/cascade/config-dev.yaml
```

Or with the container images:

```shell
# Run the NATS server.
podman run --rm -ti -p 4222:4222 docker.io/library/nats
# Run cascade with the example configuration.
podman run --rm -ti -p 5000:5000 ghcr.io/robinkb/cascade serve /usr/local/etc/config-dev.yaml
```

You can now push images to `localhost:5000`.


## Configuration

Cascade is built on CNCF Distribution, and supports the same features as Distribution.
You can refer to [Distribution's documentation](https://distribution.github.io/distribution/) for more configuration details.
The only difference is that Cascade _only_ supports the NATS storage backend that it was built for.
If you need other storage backends, you are likely better off using any of the other commonly-used registries.

NATS supports a very wide variety of deployment options.
Setting up NATS is far beyond the scope of this documentation.
Please refer to the [NATS documentation](https://docs.nats.io/running-a-nats-service/introduction) for deployment details.


## License

Cascade is [Apache 2.0 licensed](LICENSE) and accepts contributions via GitHub pull requests. Please see the [contributing guide](CONTRIBUTING.md) for more information.
