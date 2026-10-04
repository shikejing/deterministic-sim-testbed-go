package sim

import (
	"bytes"
	"encoding/gob"
)

// encodeSnapshotVersion 构造一个带有指定版本号的（ otherwise 空的）快照，
// 仅供测试版本不兼容路径。
func encodeSnapshotVersion(v uint64) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshot{Version: v}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
