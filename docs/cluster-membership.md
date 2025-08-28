# Cluster Membership

Rough notes on cluster membership in Raft.

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
So I will still need to have some way of detecting is a cluster is formed or not.
Based on that, a decision can be made to _propose_ a new node, or just adding it.
Or maybe I can take that decision away from Raft, and always use `raft.Node.ApplyConfChange`.
In any case, at least the decision can be extracted from the Node object.

I wonder what would happen we don't use `raft.Node.ProposeConfChange` and always just apply based on the service discovery.

## Remove node from a cluster

This works like joining, by calling `raft.Node.ProposeConfChange` with a `raftpb.ConfChangeV2`.
Not sure yet what this does and how it works.
