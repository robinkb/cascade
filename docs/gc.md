# Garbage Collection

# Rough Notes

What do I actually need to document?

* Problem to solve
    * Focus on the problem of GC
    * Don't make claims about other registries
    * Container image consists of at least three files
    * Deleting a manifest should delete the blobs that it points to
    * Requires tracking what blobs a manifest references
    * Blobs can be referenced by multiple manifests
    * Deleting a tag should also delete the manifest it points to
    * But multiple tags can point to one manifest
* Concept of ownership
    * Manifests are analyzed when uploaded
    * Analysis extracts which blobs the manifest points to
    * Manifest becomes owner of these blobs
    * Referrers are also processed
    * Same for image indexes
    * Tags become owners of the manifests they point to
    * Deleting a tag or manifest then removes it as owner
    * Objects without owner are deleted
    * Should also describe the cases of incomplete uploads
        * When layers are uploaded but never the manifest
        * When a manifest is never tagged
    * All this tracked in the metadata store
    * When a blob is ultimately removed from the metadata store, it is removed from the blob store
* Benefits/how it works
* The rules of garbage collection/tracking ownership
    * The ways in which manifests reference blobs and other manifests
        * Layers
        * Config
        * Index
        * Referrer
    * The fact that multiple manifests can reference the same blob
    * Multiple repositories can reference the same blob
    * Tagging
* Needing transactions to accurately track ownership

Metadata store tracks usage of blobs.
Arguably that is its primary purpose.

Metadata store is split in two parts: blobs and repositories.

The blobs in the metadata store represent physical blobs in the blob store.
Like physical blobs in the blob store, this part of the metadata store is shared between repositories.
It mainly tracks which repository owns which blobs.
A blob is owned by at least one repository.
If not, it is garbage collected.

A repository owns zero or more blobs.
Ownership is tracked in the repository by mounts.
These mounts also track if a blob was every uploaded to a repository, and if a repository should have access to the blob.
If a repository does not have a mount to a blob, then the blob cannot be read through that repository.
In turn, a mount is owned by one or more manifests in a repository.
A manifest is owned by tags and other manifests.
A tag may point to exactly one manifest.
Multiple tags may point to the same manifest.

A blob may be owned by multiple repositories.
A mount in a repository may be owned by multiple manifests within that repository.
A manifest may be owned by multiple referring or index manifests, and by multiple tags.

* Uploading a blob adds it to the blob store with the repository that it was uploaded for as an owner
    * If a blob already exists in the blob store, the repository is added as an additional owner
    * A mount to the blob is added to the repository
* Uploading a manifest adds it as an owner to:
    * Its own mount (because a manifest is also stored as a blob)
    * The mount of the referenced config blob
    * The mounts of all referenced layer blobs (if an image manifest)
    * All referenced manifests (if an index manifest)
    * The referred manifest (if a referrer manifest with a subject)
* Tagging a manifest adds the tag as an owner to the tagged manifest
    * If a tag already exists, it is removed as a owner from the manifest that it owned before
    * A tag may only reference exactly one manifest

* Deleting a tag removes it as an owner from the tagged manifest
* Deleting a manifest removes it as an owner from:
    * Its own mount
    * The mount of the referenced config blob
    * The mounts of all referenced layer blobs (if an image manifest)
    * All referenced manifests (if an index manifest)
    * The referred manifest (if a referrer manifest with a subject)
* Once a manifest has no owners, it is itself removed
* Once a mount has no owners, it is removed
* Once a blob has no owners, it is removed
* Deleting a repository removes its ownership from all mounted blobs


One gap at the moment:
If a blob is uploaded but no manifest ever references it, it remains orphaned and currently can never be deleted.


## Refinement

What you need to know for this:

* The concept of the blob store -> Simple content-addressable storage
* The layout of blobs on the filesystem? No, just that blobs are addressed by their hash.
* The different ways in which manifests point to blobs
* That manifests themselves are also blobs
* Blobs are shared across repositories
* Repositories maintain mounts to blobs as proof that they should have access
* mounts are created by uploading a blob to a repository
