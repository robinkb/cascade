# Service Discovery

Service Discovery will require leader election, likely through locking in Kubernetes.

The leader does this:

1. Create a Service for the cluster in Kubernetes.
2. Bootstrap the Raft cluster in its local node if none is formed yet.
3. Watch Endpoints and add or remove nodes from the cluster as needed.

Every node does this:

1. Create an Endpoint for itself in Kubernetes.
2. Start the Raft node once a cluster is formed.
