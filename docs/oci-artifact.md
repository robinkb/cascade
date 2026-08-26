# Storage

## Problem Statement

To understand why and how the registry stores its data, we need to understand how that data is structured.

### OCI Artifact

The format of an OCI artifact is defined in the [OCI Image spec][1].
This section contains a summary of the details necessary to understand how a registry handles artifacts.

An OCI artifact consists of:

* **Filesystem layers**: To the registry, filesystem layers are just binary blobs.
* **Configuration**: A JSON file that contains metadata about the artifact.
* **Image manifest**: A JSON file that references the configuration and layers that make up the artifact by their digests.
* **Image index**: A JSON file that references other manifests.

All of the above are identified by their digest.
A digest is a combination of an algorithm and a hash in the format `<algorithm>:<hash>`.
For example, if a manifest was hashed with the SHA256 algorithm, its digest would be something like `sha256:3b9ad...`.
Manifests refer to other objects by their digests, and a manifest is itself referenced by its digest.

An image manifest may also point to another image manifest to form a weak association.
This is called a referrer.
If an image manifest is part of a container image, then the referrer may contain metadata about that image, like a Software Bill-Of-Materials (SBOM).
A referrer points to its own layers and configuration.

Image indices are typically used for multi-platform container images.
For example, if the container image supports AMD64 and ARM64, those are actually two separate sets of layers with their own config and manifest.
An index manifest then points to those two manifests.
A client will pull the index manifest, and use it to resolve the container image for the client's platform.

Putting it all together, we get a dependency graph that looks like this:

```mermaid
graph RL
    Config
    Layers["Layer 0..n"]
    ImageManifest["Image Manifest"]
    ImageIndex["Image Index"]
    Referrer
    ReferrerConfig["Config"]
    ReferrerLayers["Layers 0..n"]

    ImageManifest --> Config
    ImageManifest --> Layers
    ImageIndex -..-> ImageManifest
    Referrer -.-> ImageManifest
    Referrer --> ReferrerConfig
    Referrer --> ReferrerLayers
```

Dotted lines indicate optional dependencies.

Manifests are typically tagged to make referring to them easier.
Often tags are semantic versions, but to the registry, they are arbitrary strings (with some limitations) that point to a manifest digest.
Tags can only every point to a single manifest, but they may be moved to another manifest.

Clients of the registry typically fetch a tag to get the digest of a manifest.
The manifest is then read to fetch the configuration and layers by their digests.
Clients may also fetch manifest by their digest directly, without first using a tag to resolve it.
This is often done for security, as a manifest digest is immutable, while tags are often mutable.

### Deduplication

Because OCI artifacts are made up out of layers, it is possible that two artifacts use some of the same layers.
For example, two container images might be built on top of the same Ubuntu base image.
It would be wasteful to upload the same data to the registry multiple times.
Some base images can be quite large, making this especially important.
That's why the registry deduplicates image layers.
Each layer is stored only once, identified by its digest.

```mermaid
graph RL
    LayerBase["Base Layer"]
    LayerA["Layer A"]
    LayerB["Layer B"]
    ImageManifestA["Image Manifest A"]
    ImageManifestB["Image Manifest B"]
    
    ImageManifestA --> LayerA
    ImageManifestA --> LayerBase
    ImageManifestB --> LayerB
    ImageManifestB --> LayerBase
```

Clients can check if a layer is already present on the registry before deciding to upload it.

### Repositories

In the registry, OCI artifacts are organized into repositories.
Given the container name `example.com/nginx:v1.2.3`:

* `example.com` is the registry host.
* `nginx` is the repository name.
* `v.1.2.3` is the version.

Each repository can hold multiple manifests.

* Layer uploaded to one repository should not be accessible through another
* However, we do want deduplication across repositories for efficiency
* That's why we have links

[1]:https://github.com/opencontainers/image-spec/blob/main/spec.md
