package raft

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	. "github.com/robinkb/cascade-registry/testing"
	"go.etcd.io/raft/v3/raftpb"
)

// Various tests exploring how cluster topology changes work in Raft.
// Most of this explored using ApplyConfChange to manage membership.
// But this is probably dangerous, because it bypasses part of the Raft algorithm.
// I'm keeping these for now, but looking into use ProposeConfChange where possible.
func TestBootstrapCluster(t *testing.T) {
	// First step: Simplify this stupid function so that peers aren't passed statically.
	// first := NewNode(id uint64, addr netip.AddrPort, peers []Peer, workDir string, snap cluster.SnapshotRestorer)
	// Next: Add method to bootstrap the cluster on the first node.
	// Basically making a single-node cluster.
	//
	// Then: Join a node through proposal.
	// Pass the URL in context first.
	// Leave service discovery for later.

	firstAddr := netip.MustParseAddrPort("127.0.0.1:50001")
	firstNode := NewNode(1, firstAddr, nil, testStore(t), &SpySnapshotter{}).(*node)

	fmt.Println(firstNode.raft.Status().Config.Voters.IDs()) // map[]

	// We have to add the node to the Raft state so that it can campaign and become the leader.
	// This is pretty much bootstrapping the cluster.
	firstNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	// The Raft status now shows the first node as part of the voters.
	fmt.Println(firstNode.raft.Status().Config.Voters.IDs()) // map[1:{}]

	firstNode.Start()

	// This works; managed to form a 1-node cluster.
	firstNode.raft.Campaign(context.Background())

	// Let's add a second node.
	secondAddr := netip.MustParseAddrPort("127.0.0.1:50002")
	secondNode := NewNode(2, secondAddr, nil, testStore(t), &SpySnapshotter{}).(*node)

	secondNode.Start()

	// Add the first node to the mesh, because it has to send messages to it.
	secondNode.mesh.SetPeer(firstNode.raft.Status().ID, firstAddr)
	// Add the first node to the known nodes.
	secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	// Now propose adding the second node to the first node.
	err := firstNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: secondNode.raft.Status().ID,
			},
		},
		Context: []byte(secondAddr.String()),
	})
	AssertNoError(t, err).Require()

	time.Sleep(1 * time.Second)

	// And about now the second node joined.
	fmt.Println(firstNode.raft.Status().Config.Voters.IDs())  // map[1:{} 2:{}]
	fmt.Println(secondNode.raft.Status().Config.Voters.IDs()) // map[1:{} 2:{}]

	// Now let's try adding a third.
	thirdAddr := netip.MustParseAddrPort("127.0.0.1:50003")
	thirdNode := NewNode(3, thirdAddr, nil, testStore(t), &SpySnapshotter{}).(*node)

	thirdNode.Start()

	// Let's try adding just the leader
	thirdNode.mesh.SetPeer(firstNode.raft.Status().ID, firstAddr)
	thirdNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	// Now propose adding the third node to the leader node.
	// It doesn't have to be the leader node. Followers can also propose conf changes.
	// The leader should be applied to the new node before joining,
	// because after the new node joins, the leader will not send a ConfChange for itself
	// to the new node. Only for other followers. If the leader is not manually added to the new node,
	// the new node will know about the leader.
	err = firstNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: thirdNode.raft.Status().ID,
			},
		},
		Context: []byte(thirdAddr.String()),
	})
	AssertNoError(t, err).Require()

	// And this is enough! Only the leader needs to be known to the new node.
	// The other nodes get shared over the messages.
	// The context of each node gets saved and shared when new nodes join.
	//
	// The problem is now figuring out which node is the first and needs to bootstrap the cluster.
	// In practice, each node can just register themselves in service discovery, and just bootstrap
	// the cluster if there are no other nodes. But this is a potential race condition. Two nodes
	// could come online and bootstrap a cluster at the same time, leaving us with two leaders.
	//
	// Actually... Let's see what happens when two leaders try to join each other.
	// Maybe Raft will handle that?
	//
	// See TestBootstrapWithTwoLeaders
	//
	// It doesn't. So let's just go with a locking mechanism.
	// Or actually it kinda does handle it when you use CheckQuorum.
	// But then a 2-node cluster never recovers. A 3-node might, but I didn't test it.
	//
	// Actually, do we need to use ProposeConfChange at all? Can't we rely on service discovery
	// and use ApplyConfChange directly instead? That's what Raft does when you use StartNode.
	// Next test case:
	// 	1. Start up a 3-node cluster.
	// 	2. Remove a node with ApplyConfChange --> Do we still have quorom? What happens in general?
	//  3. Add a node with ApplyConfChange --> Is it propagated?
	// Maybe when we use ApplyConfChange we'll have to rely on service discovery completely
	// to propagate node changes, because there are no ConfChange messages to process.
	// But that would be fine. Actually, it would simplify things a lot.
	// I just need to figure out what happens to the cluster.
	time.Sleep(1 * time.Second)

	// Removing the leader through a proposal on a follower works.
	// secondNode.raft.ProposeConfChange(context.Background(), raftpb.ConfChangeV2{
	// 	Transition: raftpb.ConfChangeTransitionAuto,
	// 	Changes: []raftpb.ConfChangeSingle{
	// 		raftpb.ConfChangeSingle{
	// 			Type:   raftpb.ConfChangeRemoveNode,
	// 			NodeID: firstNode.raft.Status().ID,
	// 		},
	// 	},
	// })

	// This removes the leader from the second node, but the first node is still the leader.
	// secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
	// 	Transition: raftpb.ConfChangeTransitionAuto,
	// 	Changes: []raftpb.ConfChangeSingle{
	// 		raftpb.ConfChangeSingle{
	// 			Type:   raftpb.ConfChangeRemoveNode,
	// 			NodeID: firstNode.raft.Status().ID,
	// 		},
	// 	},
	// })

	// If the leader node removes itself through ApplyConfChange, it steps down as a leader.
	// It also loses itself in its local cluster state, but the other nodes still recognize it.
	// It somehow also participates in leader elections.
	firstNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	// But if it's removed everywhere through ApplyConfChange, everything works as expected.
	// So I could move adding and removing nodes out of Raft and completely into the controller,
	// relying completely on service discovery. Raft will figure out a leader upon membership changes.
	// That eliminates any need for synchronization between service discovery and Raft, I think.
	// Buuut... What if there's a delay between removal on different nodes?
	// For example: node 1 gets removed. Node 2 applies the change immediately, but node 3 is slow for whatever reason.
	// Some quick tests show that node 1 can still participate in a vote, but cannot be elected.
	// It also won't be re-added to the cluster by the new leader. So it should be fine!
	//
	// But when should a node be removed from the cluster? If, for example, a node disappears for an hour,
	// then it's probably safe to remove it. But surely an expected reboot should not be cause for removal.
	// But then how _does_ one signal removal of a node? Why shouldn't an expected reboot remove a node,
	// and rejoin the cluster when it's back online?
	// Node IDs must not be re-used. But can the same node (with the same ID) join, be removed, and rejoin?
	// That would also simplify things. I need to test this.
	secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	thirdNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	time.Sleep(10 * time.Second)
}

func TestBootstrapWithTwoLeaders(t *testing.T) {
	firstAddr := netip.MustParseAddrPort("127.0.0.1:50001")
	firstNode := NewNode(1, firstAddr, nil, testStore(t), &SpySnapshotter{}).(*node)

	firstNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
	})

	secondAddr := netip.MustParseAddrPort("127.0.0.1:50002")
	secondNode := NewNode(2, secondAddr, nil, testStore(t), &SpySnapshotter{}).(*node)

	secondNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: secondNode.raft.Status().ID,
			},
		},
	})

	firstNode.Start()
	secondNode.Start()
	firstNode.raft.Campaign(context.Background())
	secondNode.raft.Campaign(context.Background())

	time.Sleep(1 * time.Second)
	// Now we have two leaders.
	// Let's try to join them and see what happens.

	err := firstNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: secondNode.raft.Status().ID,
			},
		},
		Context: []byte(secondAddr.String()),
	})
	AssertNoError(t, err).Require()

	// Second node won't recognize the first node.
	time.Sleep(1 * time.Second)

	// Let's try adding the first to the second.
	err = secondNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: firstNode.raft.Status().ID,
			},
		},
		Context: []byte(secondAddr.String()),
	})
	AssertNoError(t, err).Require()

	// Still nothing. We could probably recover from this situation,
	// but Raft won't do it automatically. We could have nodes forget
	// a leader, and start campaigning again or something.
	// But it's better not to be in this situation to begin with.
	//
	// Actually, if CheckQuorum is active, Raft will start another campaign.
	// But it won't elect another leader, probably because there's only 2 nodes.
	time.Sleep(3 * time.Second)

	thirdAddr := netip.MustParseAddrPort("127.0.0.1:50003")
	thirdNode := NewNode(3, thirdAddr, nil, testStore(t), &SpySnapshotter{}).(*node)
	thirdNode.raft.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: thirdNode.raft.Status().ID,
			},
		},
	})
	thirdNode.Start()
	thirdNode.raft.Campaign(context.Background())

	// Now we have three leaders.
	time.Sleep(1 * time.Second)

	// Try adding the second node to the third.
	thirdNode.mesh.SetPeer(secondNode.raft.Status().ID, secondAddr)
	thirdNode.mesh.SetPeer(firstNode.raft.Status().ID, firstAddr)
	err = thirdNode.raft.ProposeConfChange(context.TODO(), raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Changes: []raftpb.ConfChangeSingle{
			raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: secondNode.raft.Status().ID,
			},
		},
		Context: []byte(secondAddr.String()),
	})
	AssertNoError(t, err).Require()

	time.Sleep(10 * time.Second)

	// It's not really clear to me here what's happening. But it feels like syncing service discovery
	// with cluster state will be a challenge? Actually what is the point of using ProposeConfChange?

	fmt.Printf("%d lead: %d\n", firstNode.raft.Status().ID, firstNode.raft.Status().Lead)
	fmt.Printf("%d lead: %d\n", secondNode.raft.Status().ID, secondNode.raft.Status().Lead)
}
