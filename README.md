# Cascade Registry

Cascade is an OCI Registry built with [CNCF Distribution](https://distribution.github.io/distribution) on top of [NATS](https://nats.io).

The purpose of Cascade Registry is to be a **replicated** OCI Registry that is easy to run.
As of writing, all other OCI registries rely on object storage for replication.
Not ideal for environments that don't run in the cloud, or have limited resources.
Self-hosted S3 implementations also tend to be quite difficult to operate, or are not lightweight.
Cascade aims to solve this by using [NATS Object Store](https://docs.nats.io/nats-concepts/jetstream/obj_store) as its storage layer, and eventually by having the ability to operate NATS itself through a custom controller.

> [!IMPORTANT]
> This is experimental software.
> You should not use it for anything important.
