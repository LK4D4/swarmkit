package integration

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/docker/swarmkit/api"
	raftutils "github.com/docker/swarmkit/manager/state/raft/testutils"
	"github.com/stretchr/testify/require"
)

// pollClusterReady calls control api until all conditions are true:
// * all nodes are ready
// * all managers has membership == accepted
// * all managers has reachability == reachable
// * one node is leader
// * number of workers and managers equals to expected
func pollClusterReady(t *testing.T, c *testCluster, numWorker, numManager int) {
	pollFunc := func() error {
		res, err := c.api.ListNodes(context.Background(), &api.ListNodesRequest{})
		if err != nil {
			return err
		}
		var mCount, wCount int
		var leaderFound bool
		for _, n := range res.Nodes {
			// if node is stopped/crashed - skip it
			if c.nodes[n.ID].node == nil {
				continue
			}
			if n.Status.State != api.NodeStatus_READY {
				return fmt.Errorf("node %s with role %s isn't ready, status %s, message %s", n.ID, n.Spec.Role, n.Status.State, n.Status.Message)
			}
			if n.Spec.Membership != api.NodeMembershipAccepted {
				return fmt.Errorf("node %s with role %s isn't accepted to cluster, membership %s", n.ID, n.Spec.Role, n.Spec.Membership)
			}
			if n.Certificate.Status.State != api.IssuanceStateIssued {
				return fmt.Errorf("node %s with role %s has no issued certificate, issuance state %s", n.ID, n.Spec.Role, n.Certificate.Status.State)
			}
			if n.Spec.Role == api.NodeRoleManager {
				if n.ManagerStatus == nil {
					return fmt.Errorf("manager node %s has no ManagerStatus field", n.ID)
				}
				if n.ManagerStatus.Reachability != api.RaftMemberStatus_REACHABLE {
					return fmt.Errorf("manager node %s has reachable status: %s", n.ID, n.ManagerStatus.Reachability)
				}
				mCount++
				if n.ManagerStatus.Leader {
					leaderFound = true
				}
			} else {
				if n.ManagerStatus != nil {
					return fmt.Errorf("worker node %s should not have manager status, returned %s", n.ID, n.ManagerStatus)
				}
				wCount++
			}
		}
		if !leaderFound {
			return fmt.Errorf("leader of cluster is not found")
		}
		if mCount != numManager {
			return fmt.Errorf("unexpected number of managers: %d, expected %d", mCount, numManager)
		}
		if wCount != numWorker {
			return fmt.Errorf("unexpected number of workers: %d, expected %d", wCount, numWorker)
		}
		return nil
	}
	err := raftutils.PollFuncWithTimeout(nil, pollFunc, opsTimeout)
	require.NoError(t, err)
}

func pollServiceReady(t *testing.T, c *Cluster, sid string, tasksNum int) {
	pollFunc := func() error {
		req := &api.ListTasksRequest{}
		res, err := c.api.ListTasks(context.Background(), req)
		require.NoError(t, err)
		if len(res.Tasks) != tasksNum {
			return fmt.Errorf("unexpected number of tasks: %d, expected %d", len(res.Tasks), tasksNum)
		}
		for _, task := range res.Tasks {
			if task.Status.State != api.TaskStateRunning {
				return fmt.Errorf("task %s is not running, status %s", task.ID, task.Status.State)
			}
		}
		return nil
	}
	require.NoError(t, raftutils.PollFuncWithTimeout(nil, pollFunc, opsTimeout))
}

func newCluster(t *testing.T, numWorker, numManager int) *testCluster {
	cl := newTestCluster()
	for i := 0; i < numManager; i++ {
		require.NoError(t, cl.AddManager(), "manager number %d", i+1)
	}
	for i := 0; i < numWorker; i++ {
		require.NoError(t, cl.AddAgent(), "agent number %d", i+1)
	}

	pollClusterReady(t, cl, numWorker, numManager)
	return cl
}

func TestClusterCreate(t *testing.T) {
	numWorker, numManager := 2, 3
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()
}

func TestServiceCreate(t *testing.T) {
	numWorker, numManager := 15, 5
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	numTasks := 60
	sid, err := cl.CreateService("test_service", numTasks)
	require.NoError(t, err)
	pollServiceReady(t, cl, sid, 60)
}

func TestNodeOps(t *testing.T) {
	numWorker, numManager := 1, 3
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	// demote leader
	leader, err := cl.Leader()
	require.NoError(t, err)
	require.NoError(t, cl.SetNodeRole(leader.node.NodeID(), api.NodeRoleWorker))
	// agents 2, managers 2
	numWorker++
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)

	// remove node
	var worker *testNode
	for _, n := range cl.nodes {
		if !n.config.IsManager && n.node.NodeID() != leader.node.NodeID() {
			worker = n
			break
		}
	}
	require.NoError(t, cl.RemoveNode(worker.node.NodeID()))
	// agents 1, managers 2
	numWorker--
	// long wait for heartbeat expiration
	pollClusterReady(t, cl, numWorker, numManager)

	// promote old leader back
	require.NoError(t, cl.SetNodeRole(leader.node.NodeID(), api.NodeRoleManager))
	numWorker--
	numManager++
	// agents 0, managers 3
	pollClusterReady(t, cl, numWorker, numManager)
}

func TestDemotePromoteDemote(t *testing.T) {
	numWorker, numManager := 1, 3
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	leader, err := cl.Leader()
	require.NoError(t, err)
	var manager *testNode
	for _, n := range cl.nodes {
		if n.config.IsManager && n.node.NodeID() != leader.node.NodeID() {
			manager = n
			break
		}
	}
	require.NoError(t, cl.SetNodeRole(manager.node.NodeID(), api.NodeRoleWorker))
	// agents 2, managers 2
	numWorker++
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)

	// promote same node
	require.NoError(t, cl.SetNodeRole(manager.node.NodeID(), api.NodeRoleManager))
	// agents 1, managers 3
	numWorker--
	numManager++
	pollClusterReady(t, cl, numWorker, numManager)

	require.NoError(t, cl.SetNodeRole(manager.node.NodeID(), api.NodeRoleWorker))
	// agents 2, managers 2
	numWorker++
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)
}

func TestPromoteDemotePromote(t *testing.T) {
	numWorker, numManager := 1, 3
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	var worker *testNode
	for _, n := range cl.nodes {
		if !n.config.IsManager {
			worker = n
			break
		}
	}
	require.NoError(t, cl.SetNodeRole(worker.node.NodeID(), api.NodeRoleManager))
	// agents 0, managers 4
	numWorker--
	numManager++
	pollClusterReady(t, cl, numWorker, numManager)

	// demote same node
	require.NoError(t, cl.SetNodeRole(worker.node.NodeID(), api.NodeRoleWorker))
	// agents 1, managers 3
	numWorker++
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)

	require.NoError(t, cl.SetNodeRole(worker.node.NodeID(), api.NodeRoleManager))
	// agents 0, managers 4
	numWorker--
	numManager++
	pollClusterReady(t, cl, numWorker, numManager)
}

func TestDemotePromoteLeader(t *testing.T) {
	numWorker, numManager := 1, 3
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	leader, err := cl.Leader()
	require.NoError(t, err)
	require.NoError(t, cl.SetNodeRole(leader.node.NodeID(), api.NodeRoleWorker))
	// agents 2, managers 2
	numWorker++
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)

	//promote former leader back
	require.NoError(t, cl.SetNodeRole(leader.node.NodeID(), api.NodeRoleManager))
	// agents 1, managers 3
	numWorker--
	numManager++
	pollClusterReady(t, cl, numWorker, numManager)
}

func TestDemoteToSingleManagerAndBack(t *testing.T) {
	numWorker, numManager := 1, 3
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	leader, err := cl.Leader()
	require.NoError(t, err)
	require.NoError(t, cl.SetNodeRole(leader.node.NodeID(), api.NodeRoleWorker))
	// agents 2, managers 2
	numWorker++
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)

	leader, err = cl.Leader()
	require.NoError(t, err)
	require.NoError(t, cl.SetNodeRole(leader.node.NodeID(), api.NodeRoleWorker))
	// agents 3, managers 1
	numWorker++
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)

	for _, n := range cl.nodes {
		if !n.config.IsManager {
			require.NoError(t, cl.SetNodeRole(n.node.NodeID(), api.NodeRoleManager))
			numWorker--
			numManager++
			pollClusterReady(t, cl, numWorker, numManager)
		}
	}
}

func TestRestartLeader(t *testing.T) {
	numWorker, numManager := 1, 3
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	// stop leader
	leader, err := cl.Leader()
	require.NoError(t, err)
	require.NoError(t, leader.Stop())
	// agents 1, managers 2
	numManager--
	pollClusterReady(t, cl, numWorker, numManager)

	require.NoError(t, leader.StartAgain(cl.ctx))

	// agents 1, managers 3
	numManager++
	pollClusterReady(t, cl, numWorker, numManager)
}

func TestRestartCluster(t *testing.T) {
	numWorker, numManager := 1, 5
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	for _, n := range cl.nodes {
		if n.config.IsManager {
			require.NoError(t, n.Stop())
		}
	}

	for _, n := range cl.nodes {
		require.NoError(t, n.StartAgain(cl.ctx))
	}

	pollClusterReady(t, cl, numWorker, numManager)
}

func TestRestartClusterOneByOne(t *testing.T) {
	numWorker, numManager := 1, 5
	cl := newCluster(t, numWorker, numManager)
	defer func() {
		require.NoError(t, cl.Stop())
	}()

	for _, n := range cl.nodes {
		if n.config.IsManager {
			require.NoError(t, n.Stop())
			require.NoError(t, n.StartAgain(cl.ctx))
		}
	}
	pollClusterReady(t, cl, numWorker, numManager)
}
