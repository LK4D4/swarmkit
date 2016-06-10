package integration

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/docker/swarmkit/api"
	raftutils "github.com/docker/swarmkit/manager/state/raft/testutils"
	"golang.org/x/net/context"
)

const opsTimeout = 32 * time.Second

// Cluster is representation of cluster - connected nodes.
type Cluster struct {
	ctx    context.Context
	cancel context.CancelFunc
	api    *dummyAPI
	nodes  map[string]*TestNode
	errs   chan error
	wg     sync.WaitGroup
}

// NewCluster creates new cluster to which nodes can be added.
// AcceptancePolicy is set to most permissive mode on first manager node added.
func NewCluster() *Cluster {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{
		ctx:    ctx,
		cancel: cancel,
		nodes:  make(map[string]*TestNode),
		errs:   make(chan error, 1024),
	}
	c.api = &dummyAPI{c: c}
	return c
}

// Stop makes best effort to stop all nodes and close connections to them.
func (c *Cluster) Stop() error {
	c.cancel()
	for _, n := range c.nodes {
		if err := n.Stop(); err != nil {
			return err
		}
	}
	c.wg.Wait()
	close(c.errs)
	for err := range c.errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// RandomManager choses random manager from cluster.
func (c *Cluster) RandomManager() *TestNode {
	var managers []*TestNode
	for _, n := range c.nodes {
		if n.config.IsManager {
			managers = append(managers, n)
		}
	}
	idx := rand.Intn(len(managers))
	return managers[idx]
}

// AddManager adds node with Manager role(both agent and manager).
func (c *Cluster) AddManager() error {
	// first node
	var n *TestNode
	if len(c.nodes) == 0 {
		node, err := newNode(true, "")
		if err != nil {
			return err
		}
		n = node
	} else {
		joinAddr, err := c.RandomManager().node.RemoteAPIAddr()
		if err != nil {
			return err
		}
		node, err := newNode(true, joinAddr)
		if err != nil {
			return err
		}
		n = node
	}
	c.wg.Add(1)
	go func() {
		c.errs <- n.node.Start(c.ctx)
		c.wg.Done()
	}()

	select {
	case <-n.node.Ready():
	case <-time.After(opsTimeout):
		return fmt.Errorf("node did not ready in time")
	}

	// change acceptance policy on cluster creation
	if len(c.nodes) == 0 {
		// we retry this, because sequence number can change
		var ok bool
		for retry := 0; retry < 5; retry++ {
			if err := n.SetAcceptancePolicy(); err != nil {
				if grpc.ErrorDesc(err) == "update out of sequence" {
					continue
				}
				return fmt.Errorf("set acceptance policy: %v", err)
			}
			ok = true
			break
		}
		if !ok {
			return fmt.Errorf("set acceptance policy, got sequence error 5 times")
		}
	}
	c.nodes[n.node.NodeID()] = n
	return nil
}

// AddAgent adds node with Agent role(doesn't participate in raft cluster).
func (c *Cluster) AddAgent() error {
	// first node
	var n *TestNode
	if len(c.nodes) == 0 {
		return fmt.Errorf("there is no manager nodes")
	}
	joinAddr, err := c.RandomManager().node.RemoteAPIAddr()
	if err != nil {
		return err
	}
	node, err := newNode(false, joinAddr)
	if err != nil {
		return err
	}
	n = node
	c.wg.Add(1)
	go func() {
		c.errs <- n.node.Start(c.ctx)
		c.wg.Done()
	}()

	select {
	case <-n.node.Ready():
	case <-time.After(opsTimeout):
		return fmt.Errorf("node is not ready in time")
	}
	c.nodes[n.node.NodeID()] = n
	return nil
}

// CreateService creates dummy service.
func (c *Cluster) CreateService(name string, instances int) (string, error) {
	spec := &api.ServiceSpec{
		Annotations: api.Annotations{Name: name},
		Mode: &api.ServiceSpec_Replicated{
			Replicated: &api.ReplicatedService{
				Replicas: uint64(instances),
			},
		},
		Task: api.TaskSpec{
			Runtime: &api.TaskSpec_Container{
				Container: &api.ContainerSpec{Image: "alpine", Command: []string{"sh"}},
			},
		},
	}

	resp, err := c.api.CreateService(context.Background(), &api.CreateServiceRequest{Spec: spec})
	if err != nil {
		return "", err
	}
	return resp.Service.ID, nil
}

// Leader returns TestNode for cluster leader.
func (c *Cluster) Leader() (*TestNode, error) {
	resp, err := c.api.ListNodes(context.Background(), &api.ListNodesRequest{
		Filters: &api.ListNodesRequest_Filters{
			Roles: []api.NodeRole{api.NodeRoleManager},
		},
	})
	if err != nil {
		return nil, err
	}
	for _, n := range resp.Nodes {
		if n.ManagerStatus.Leader {
			tn, ok := c.nodes[n.ID]
			if !ok {
				return nil, fmt.Errorf("leader id is %s, but it isn't found in test cluster object", n.ID)
			}
			return tn, nil
		}
	}
	return nil, fmt.Errorf("cluster leader is not found in storage")
}

// RemoveNode removes node entirely. It tries to demote managers.
func (c *Cluster) RemoveNode(id string) error {
	node, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("remove node: node %s not found", id)
	}
	// demote before removal
	if node.config.IsManager {
		if err := c.SetNodeRole(id, api.NodeRoleWorker); err != nil {
			return fmt.Errorf("demote manager: %v", err)
		}

	}
	if err := node.Stop(); err != nil {
		return err
	}
	delete(c.nodes, id)
	if err := raftutils.PollFuncWithTimeout(nil, func() error {
		resp, err := c.api.GetNode(context.Background(), &api.GetNodeRequest{NodeID: id})
		if err != nil {
			return fmt.Errorf("get node: %v", err)
		}
		if resp.Node.Status.State != api.NodeStatus_DOWN {
			return fmt.Errorf("node %s is still not down", id)
		}
		return nil
	}, opsTimeout); err != nil {
		return err
	}
	if _, err := c.api.RemoveNode(context.Background(), &api.RemoveNodeRequest{NodeID: id}); err != nil {
		return fmt.Errorf("remove node: %v", err)
	}
	return nil
}

// SetNodeRole sets role for node through control api.
func (c *Cluster) SetNodeRole(id string, role api.NodeRole) error {
	node, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("set node role: node %s not found", id)
	}
	if node.config.IsManager && role == api.NodeRoleManager {
		return fmt.Errorf("node is already manager")
	}
	if !node.config.IsManager && role == api.NodeRoleWorker {
		return fmt.Errorf("node is already worker")
	}

	// version might change between get and update, so retry
	for i := 0; i < 5; i++ {
		resp, err := c.api.GetNode(context.Background(), &api.GetNodeRequest{NodeID: id})
		if err != nil {
			return err
		}
		spec := resp.Node.Spec.Copy()
		spec.Role = role
		if _, err := c.api.UpdateNode(context.Background(), &api.UpdateNodeRequest{
			NodeID:      id,
			Spec:        spec,
			NodeVersion: &resp.Node.Meta.Version,
		}); err != nil {
			// there possible problems on calling update node because redirecting
			// node or leader might want to shut down
			if grpc.ErrorDesc(err) == "update out of sequence" || grpc.ErrorDesc(err) == "grpc: the client connection is closing" {
				continue
			}
			return err
		}
		node.config.IsManager = role == api.NodeRoleManager
		return nil
	}
	return fmt.Errorf("set role %s for node %s, got sequence error 5 times", role, id)
}
