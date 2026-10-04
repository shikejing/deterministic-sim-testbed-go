package sim

// LogEntry 记录一条已执行事件及其执行后的状态哈希。
type LogEntry struct {
	Step  uint64
	Event Event
	Hash  uint64 // 执行该事件后的全状态哈希
	Note  string // 故障注入点等说明
}

// EffectEntry 记录一次对外效应（重放时只校验、不重放）。
type EffectEntry struct {
	Step   uint64
	Effect string
	Data   any
}

// EventLog 是运行期的事件日志。
// 容量受 Config.LogCap 限制：达到上限后淘汰最旧条目（环形），
// 保证长时间运行内存可控；cap<=0 的语义见 newEventLog。
type EventLog struct {
	entries []LogEntry
	effects []EffectEntry
	head    int // 环形缓冲起点（仅 cap>0 时启用）
	full    bool
	cap     int // 0 = 不记录；<0 = 不限
}

func newEventLog(cap int) *EventLog {
	l := &EventLog{cap: cap}
	if cap > 0 {
		l.entries = make([]LogEntry, 0, cap)
	}
	return l
}

func (l *EventLog) record(e LogEntry) {
	if l.cap == 0 {
		return
	}
	if l.cap < 0 { // 不限
		l.entries = append(l.entries, e)
		return
	}
	if !l.full {
		l.entries = append(l.entries, e)
		if len(l.entries) == l.cap {
			l.full = true
			l.head = 0
		}
		return
	}
	l.entries[l.head] = e
	l.head = (l.head + 1) % l.cap
}

func (l *EventLog) recordEffect(e EffectEntry) {
	l.effects = append(l.effects, e)
}

// Entries 按时间顺序返回日志条目（容量受限时为最近若干条）。
func (l *EventLog) Entries() []LogEntry {
	if !l.full || l.cap <= 0 {
		out := make([]LogEntry, len(l.entries))
		copy(out, l.entries)
		return out
	}
	out := make([]LogEntry, 0, l.cap)
	out = append(out, l.entries[l.head:]...)
	out = append(out, l.entries[:l.head]...)
	return out
}

// Effects 返回对外效应日志。
func (l *EventLog) Effects() []EffectEntry {
	out := make([]EffectEntry, len(l.effects))
	copy(out, l.effects)
	return out
}
