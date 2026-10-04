package sim

// Node 是被仿真的实体。实现必须满足：
//  1. 行为只依赖收到的 Event 与 Context 提供的能力（Now/Rand），
//     不得读取真实时钟、真实网络或包级随机源；
//  2. 状态可通过 Snapshot/Restore 完整往返。
type Node interface {
	ID() NodeID
	// Init 在节点加入仿真或重启后被调用，可设置初始定时器等。
	Init(ctx *Context)
	// OnEvent 处理一个事件（消息送达、定时器到期、客户端请求）。
	OnEvent(ctx *Context, ev Event)
	// Snapshot 序列化节点全部状态。
	Snapshot() ([]byte, error)
	// Restore 从 Snapshot 的数据恢复。
	Restore(data []byte) error
}

// Context 是节点与仿真器交互的唯一入口。
// 节点只能通过它感知时间、获取随机性、发送消息与设置定时器，
// 从而保证全部不确定性都由仿真器统一控制。
type Context struct {
	sim  *Simulator
	from NodeID
}

// Now 返回当前虚拟时间（含已注入的时钟跳变偏移）。
func (c *Context) Now() Time { return c.sim.now + c.sim.clockOffset }

// Rand 返回仿真级确定性随机源。
// 注意：节点消费随机数会进入全局确定序列，同一事件序列下结果一致。
func (c *Context) Rand() *RNG { return c.sim.rng }

// Send 经仿真网络向 to 发送一条消息。
// 发送点即故障注入点：可能被丢失、复制或追加乱序延迟。
func (c *Context) Send(to NodeID, kind string, body any) {
	c.sim.sendMessage(c.from, to, Message{Kind: kind, Body: body})
}

// After 在 delay 个滴答后向自己投递一个定时器事件。
func (c *Context) After(delay Time, payload any) {
	c.sim.setTimer(c.from, delay, payload)
}

// Crashed 报告某节点当前是否处于崩溃状态。
func (c *Context) Crashed(id NodeID) bool { return c.sim.Crashed(id) }

// Emit 记录一条应用层可见的“对外效应”（如响应客户端），
// 仅写入事件日志；重放时只校验、不会再次触发真实副作用。
func (c *Context) Emit(effect string, data any) {
	e := EffectEntry{Step: c.sim.step, Effect: effect, Data: data}
	if c.sim.replayMode {
		c.sim.replayEffects = append(c.sim.replayEffects, e)
		return
	}
	c.sim.log.recordEffect(e)
}
