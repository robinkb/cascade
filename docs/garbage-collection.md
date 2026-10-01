# Garbage Collection

Describing how garbage collection works.

## Required Reading

Before you continue, make sure to have read the following documents:

* [Store Design](./store-design.md)

## Object Relationships

As described in the store design, there are several types of objects in the registry that reference each other in various ways:

* Tags point to index or image manifests.
* Index manifests point to image manifests.
* Image manifests point to configurations and layers.
* Referrers point to image manifests and its own configurations and layers.
* Blobs are shared across container images and repositories.

This leads to some complications when it comes to deleting data from the registry.
First, any dangling objects should automatically be cleaned up.
If a manifest is deleted, anything referencing that manifest and anything referenced by that manifest should also be deleted.
Otherwise, data will accumulate in the registry indefinitely.
Second, layers that are in-use should be protected from deletion.
Deleting a layer when it's still in use could break multiple container images across several repositories.
When deleting a manifest, it is not safe to simply delete all the layers that it references, because those layers may be referenced by other manifests.

The simplest way to clean up dangling objects is to do stop-the-world garbage collection.
Writing to the registry is disabled, after which a garbage collector scans all the tags and manifests in the registry.
The garbage collector tracks which manifests are not referenced by tags, and which blobs are not referenced by manifests.
These unreferenced objects are then deleted, and writing to the registry can be enabled again.
If writing to the registry were not disabled during this process, the garbage collector would be unable to build an accurate view of the various dependencies in the registry.

This requires more conscious management of the registry and is, simply put, a bit annoying.

## Ownership

Cascade instead implements a strategy inspired by Rust's ownership model.
A registry is not nearly as complex as a programming language, so it is quite simple.
It follows these rules:

* Every object in the registry must have an owner.
* An object may have multiple owners.
* An object with an owner may not be removed.
* When an owner is removed, it is removed as the owner from any objects that it owns.
* An object without owners is removed.

Concretely, the registry has the following ownership relationships:

* Blobs are owned by one or more repositories.
* Mounts are owned by one or more manifests.
* Image manifests are owned by tags or index manifests.
* Index manifests are owned by one or more tags.
* Referrers are owned by their subjects.

It is the metadata store's responsibility to track these ownerships.
Whenever any object is created in the registry, the metadata store keeps track of what it owns.
Likewise, whenever an object is removed, the metadata store removes its ownerships.
Objects that are owned are protected from deletion, and objects that are ownerless are marked for deletion.

There is one exception to the rule: manifests.
Manifests are owned by tags, and thus a tagged manifest should not be deletable.
However, some registry interfaces "delete" a tag by deleting the manifest that it points to.
In order to be compatible with these tools, Cascade allows deleting a tagged manifest, and will delete all tags that point to the manifest.

Uploading a container image is not an atomic operation, so objects can still remain ownerless under this model.

* An upload may never get finished.
* A configuration or layer may be uploaded, but never a manifest that references it.
* A manifest may be uploaded, but never tagged.

These situations still require a garbage collector, but it does not require stopping the world.
The registry should instead mark the creation date of each object.
If after a certain amount of time an object still does not have an owner, it can safely be removed.
