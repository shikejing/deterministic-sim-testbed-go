package sim

// NodeID 唯一标识一个仿真节点。
type NodeID string

// Message 是节点间传递的消息。
type Message struct {
	From    NodeID
	To      NodeID
	Payload []byte
}

// Node 是被仿真的实体。所有方法都在仿真主循环中单线程调用，
// 实现里不得使用真实时钟、真实网络或全局随机源。
type Node interface {
	// Init 在节点加入引擎或崩溃重启后调用。
	Init(ctx *Context)
	// OnMessage 处理一条送达的消息。
	OnMessage(msg Message)
	// OnTimer 处理一个到期的定时器。
	OnTimer(name string)
	// OnCrash 在节点崩溃时调用，用于清理易失状态。
	OnCrash()
	// Snapshot 序列化节点状态。
	Snapshot() ([]byte, error)
	// Restore 从快照恢复节点状态。
	Restore(data []byte) error
}

// ContextAttacher 是可选接口：引擎在注册、恢复、重启时把
// 上下文挂回节点，节点可借此保存 *Context 供后续回调使用。
type ContextAttacher interface {
	AttachContext(ctx *Context)
}

// Context 是节点与引擎交互的唯一接口。
type Context struct {
	engine *Engine
	self   NodeID
}

// Now 返回该节点看到的虚拟时间（含时钟跳变注入的偏移）。
func (c *Context) Now() int64 {
	ns := c.engine.nodes[c.self]
	if ns == nil {
		return c.engine.now
	}
	return c.engine.now + ns.offset
}

// Self 返回当前节点 ID。
func (c *Context) Self() NodeID { return c.self }

// Send 向目标节点发送消息，投递时间与是否送达由仿真网络决定。
// payload 会被拷贝，节点可在调用后复用缓冲区。
func (c *Context) Send(to NodeID, payload []byte) {
	cp := append([]byte(nil), payload...)
	c.engine.sendMessage(c.self, to, cp)
}

// SetTimer 注册一个 delay（虚拟纳秒）后触发的定时器。
func (c *Context) SetTimer(name string, delay int64) {
	c.engine.setTimer(c.self, name, delay)
}
