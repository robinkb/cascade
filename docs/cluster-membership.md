# Cluster Membership

Notes on managing the cluster topology in `etcd-io/raft`.


## Static Cluster Topology

The easiest way to form a cluster with Raft, is by using static list of nodes.
When each node starts, it is passed an ID and a socket address (an address and a port).
It's also passed a list of all peers in the cluster, each represented by an ID and socket address.
Each node is started and campaigns through Raft to elect a leader, and form a cluster.

In code, this works by calling `raft.StartNode` with a `raft.Config` and list of `raft.Peer`.
Under the hood, `raft.StartNode` bootstraps the node's storage with internal interfaces to add ConfChange entries.
These ConfChange entries add the passed peers into the Raft state, ensuring that nodes can start campaigning.
Without this bootstrapping process, nodes would not know that there are peers to vote with and form individual "clusters".

When a node is _restarted_, `raft.RestartNode` should be called instead.
This function is the same as `raft.StartNode`, except it does not manipulate the node's state.
In fact, `raft.StartNode` contains safeguards to ensure that a non-empty state is not manipulated.
You could ignore `raft.RestartNode` and call `raft.StartNode` every time, relying on the safeguard.


## Limitations

A static cluster is great for starting to develop with Raft, but quickly causes limitations.
Testing replication of data is easy, but any cluster needs to support replacing nodes.
Developing, and especially testing, snapshotting and recovery is hard when the cluster is static.
In fact, one of the major goals of Cascade is allowing to start with one node, and growing it into a cluster as needed.

A dynamic topology is thus a hard requirement, and Raft supports this just fine.
All that is needed is to understand the details of how it all works.


## Topology Changes

Changes to the topology can be proposed with `raft.Node.ProposeConfChange`.
This method takes a `raftpb.ConfChangeV2` struct, detailing whether a node identified by its ID should be added, removed, or updated.
A `raftpb.ConfChange` struct can also be used, but that is deprecated, so let's ignore it.
Once a proposal has been accepted by the leader, is it propagated to each node in the cluster.
The accepted proposal must be passed to `raft.Node.ApplyConfChange` on each node.
As the name implies, this actually applies the proposed change to the cluster.

Proposals have a "Context" field, of type `[]byte`.
This can hold arbitrary data that is associated with the node.
When the proposal is propagated to every node, this context is sent along with it.
It can be used to contain the network socket (or any other data) of the node that the proposal is about.

Proposals can be made through any node with `etcd-io/raft`; it need not be the leader.
If the node is a follower, the proposal is forward to the leader and evaluated there.
The cluster's leader may reject proposals through a number of safeguards that I honestly do not understand.

One could bypass the proposal process (and the safeguards) by calling `raft.Node.ApplyConfChange` directly.
It is not strictly necessary to go through the proposal process.
This could be useful in certain cases that I have not yet defined, where direct control over the cluster topology is needed.
It allows something similar to the bootstrapping in `raft.StartNode`, but with more control, and even with a non-empty state.
Bypassing the safeguards likely does pose some risks, and would require precise testing.

Of note is that a new node does not _have_ to be bootstrapped with the full list of peers, or even any peers.
It is enough to bootstrap only the leader into its state, but the leader is required.
Once the new node joins the cluster, it receives info about the other nodes (including _itself_!) from the leader.
However, the leader does not propagate information about itself.
It assumes that any follower already knows the leader.
If the leader is not bootstrapped into a new node's state, the leader will not be known until the leadership changes.

It is safer and simpler to just bootstrap a node's state with all known nodes, including itself.
This avoids any issues with leadership changes between bootstrapping a node, and it actually joining the cluster.
It also eliminates having to figure out which node is the leader.


## Decoupling Start and Bootstrap

As outlined earlier, the responsibilities of `raft.StartNode` are actually two-fold.
It starts the Raft node, but it also bootstraps the Raft state with the given peers.

In Cascade, a Raft node is always started by calling `raft.RestartNode`, and never `raft.StartNode`.
Bootstrapping the Raft state is instead done by calling `raft.Node.ApplyConfChange` to add peers.
This allows a decoupling of starting and bootstrapping nodes.


## Dynamic Cluster Topology

Knowing all that, the process of starting a dynamic cluster is straight-forward.

These actions on nodes are defined:

1. Start: The act of starting the Raft state machine, and the necessary go routines to process its messages.
2. Bootstrap: Manipulating the node's state to make the given peers known to it.

The process of starting a cluster and growing it is as follows:

1. Node One is started and then bootstrapped with itself as the peer.
2. Node One starts campaigning and, as the only member, elects itself as leader.
3. Node One at this point forms a single-node cluster.
4. Node Two is started and then bootstrapped with itself and node One as peers.
5. A proposal is submitted on node One to add node Two to the cluster.
6. The proposal is accepted, and node Two joins the cluster as a follower.
7. Node Three is started and then bootstrapped with itself, node One, and node Two as peers.
8. A proposal is submitted on either node One or Two to add node Three to the cluster.
9. The proposal is accepted, and node Three joins the cluster as a follower.

With this knowledge, we can resize clusters for test scenarios, and starting building a controller to automate it all.
