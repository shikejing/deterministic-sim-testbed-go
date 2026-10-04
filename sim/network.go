package sim

// network 建模节点间链路的分区状态。
// 消息丢失/重复/乱序在发送时由 faultInjector 决定并直接体现在
// 事件队列中；network 只负责“当前链路是否连通”这一时变状态。
type network struct {
	// blocked 记录被分区切断的方向性链路 src->dst。
	blocked map[[2]NodeID]bool
}

func newNetwork() *network { return &network{blocked: map[[2]NodeID]bool{}} }

// cut 切断 src->dst 方向链路。
func (n *network) cut(src, dst NodeID) { n.blocked[[2]NodeID{src, dst}] = true }

// heal 恢复 src->dst 方向链路。
func (n *network) heal(src, dst NodeID) { delete(n.blocked, [2]NodeID{src, dst}) }

// healAll 恢复全部链路。
func (n *network) healAll() { n.blocked = map[[2]NodeID]bool{} }

// reachable 报告 src->dst 当前是否可达。
func (n *network) reachable(src, dst NodeID) bool {
	return !n.blocked[[2]NodeID{src, dst}]
}

// snapshot / restore 用于状态保存（返回副本，避免共享底层 map）。
func (n *network) snapshot() map[[2]NodeID]bool {
	m := make(map[[2]NodeID]bool, len(n.blocked))
	for k, v := range n.blocked {
		m[k] = v
	}
	return m
}

func (n *network) restore(m map[[2]NodeID]bool) {
	n.blocked = make(map[[2]NodeID]bool, len(m))
	for k, v := range m {
		n.blocked[k] = v
	}
}
