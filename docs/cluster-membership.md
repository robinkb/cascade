# Cluster Membership

Notes and findings on management cluster membership in Raft.

## Current Situation

In the current state of Cascade, Raft cluster membership is static.
Raft nodes are started with `raft.StartNode`, passing the full peer list as an argument.
Once the node is started, there is no way to add or remove nodes wihout restarting.

Under the hood, `raft.StartNode` manipulates the unstable storage of the `raft.RawNode` underlying the `raft.Node` to add the given peers directly.
This bootstrapping is necessary when starting a new node.
When the node joins the cluster, information about the new follower is propagated to all existing followers.
Likewise, information about other followers is propagated to the new follower.
However, the leader does not propagate information about itself to the new follower.
It assumes that the new follower already knows about the leader.

## Topology Changes

Changes to the topology can be made by passing `raftpb.ConfChangeV2` structs to `raft.Node.ProposeConfChange` and `raft.Node.ApplyConfChange`.
These structs and methods are specifically for making changes to the cluster topology.

Proposing a change through `raft.Node.ProposeConfChange` relies on Raft consensus to change the topology.
It may be rejected depending on the state of the cluster.

Applying a change through `raft.Node.ApplyConfChange` without proposing it first bypasses the Raft algorithm, and the log.
It would be lost after a restart, and it may bypass some safeguards.
They are also not propagated to other nodes.



## Static Topology

The cluster topology describes which peers are a part of the cluster.

In the most simple configuration, the topology is static.
In a static topology, a node is started with `raft.StartNode`,
This function takes a full list of peers as an argument, returning a `raft.Node`.
The returned `raft.Node` is initialized so that it already knows of each peer passed to `raft.StartNode`.
The node will attempt to find its peers and exchange messages to elect a leader.

Under the hood `raft.Node` wraps `raft.RawNode`.
The `raft.Node` returned by `raft.StartNode` has its state manipulated to add the passed peers.
The methods to do so are not public.

## Topology Change Proposals

Once the cluster is formed, changes to the topology can be proposed through `raft.Node.ProposeConfChange`.
Multiple changes can be proposed at once, and changes have one of these types:

* `raftpb.ConfChangeAddNode`
*	`raftpb.ConfChangeRemoveNode`
* `raftpb.ConfChangeUpdateNode`
* `raftpb.ConfChangeAddLearnerNode`

I don't really understand what a learner node is, so let's ignore it.

Besides the type, a `ConfChange` includes the ID of the node being changed, and a context payload associated with that node.

Once a change is accepted by the cluster,

## Join node to a cluster

### What is the difference between `raft.StartNode` and `raft.RestartNode`?

Only one. `raft.StartNode` calls `raft.RawNode.Bootstrap`.
This method generates a `ConfChange` `Entry` for every `raft.Peer` passed to `raft.StartNode`.
These `Entries` get applied directly to the unstable log.

### How does joining a node in a running cluster work?

Generate a `raftpb.ConfChangeV2` containing the new node's ID, and any context.
This context can be any data belonging to the node, like a URL.
Really whatever.

This `raftpb.ConfChange` gets passed to `raft.Node.ProposeConfChange`

Can I start nodes with `raft.RestartNode` and pass in the peers afterwards?
Let's find out!

-> I can!

This should let me make starting a node more straightforward.
It probably also simplifies reconciling discovered peers with Raft peers.

However, to bootstrap the cluster, I need to use `raft.Node.ApplyConfChange`.
This makes sense, as there is not yet a cluster to propose changes to.
So I will still need to have some way of detecting if a cluster is formed or not.
Based on that, a decision can be made to _propose_ a new node, or just adding it.
Or maybe I can take that decision away from Raft, and always use `raft.Node.ApplyConfChange`.
In any case, at least the decision can be extracted from the Node object.

I wonder what would happen we don't use `raft.Node.ProposeConfChange` and always just apply based on the service discovery.

## Remove node from a cluster

This works like joining, by calling `raft.Node.ProposeConfChange` with a `raftpb.ConfChangeV2`.
Not sure yet what this does and how it works.
