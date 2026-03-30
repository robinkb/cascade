# Store

Describing the various stores in use in the registry.

## Metadata

- Blobs
  - <digest>
    - owners
        - <repository>
- Repositories
  - Blobs
  - Manifests
  - Tags

Tags reference manifests.
Tags only exist as metadata.
Manifests reference blobs through `Layers` field, a config file through the `Config` field, or other manifests (through `Subject` field).
There's also the case of index manifests, which reference other manifests through the `Manifests` field.
Manifests themselves are blobs in the blob store.
Blobs in the repository reference blobs in the blob store.

The top-level blobs tracks all blobs that are present in the shared blob store.

The model of the metadata store is heavily influenced by the goal of achieving online garbage collection.
Online garbage collection means that any unused resources are automatically cleaned up.
In other words, if a user deletes a tag in a repository, the manifest that it points to is deleted.
The blobs that the manifest points to are also deleted automatically.

Online garbage collection is enabled by tracking ownership between entities in the metadata store.
This "ownership" model is loosely inspired by Rust.

Tags are generally deleted by a user.
Automatic cleanup could be built for use cases such as:

- Deleted a tag if it hasn't been pulled in N days.
- Delete a tag that does not match a pattern after N days.

Manifests or blobs are generally not deleted by users.
In fact, they should not be.
If a manifest gets deleted directly, all tags that point to it would become invalid.
If a blob gets deleted directly, all manifests that point to it would become invalid.
The spec allows it, but this only makes sense for registries without built-in garbage collection.
In such cases, an external process would have to scan the registry to perform garbage collection.
It makes sense that if automatic garbage collection is enabled, that manual manifest and blob deletion is disabled.
Or there could be extra checks to confirm that no manifests or tags would become orphaned by a deletion.
But that is more complex and more compulationally expensive, and I see no real reason for it to exist.
