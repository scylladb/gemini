package gemini

import (
	"bytes"
	"encoding/binary"

	"github.com/gocql/gocql"
)

type RoutingKeyCreator struct {
	routingKeyBuffer []byte
}

func (rc *RoutingKeyCreator) CreateRoutingKey(table *Table, values []interface{}) ([]byte, error) {
	partitionKeys := table.PartitionKeys
	if len(partitionKeys) == 1 {
		// single column routing key
		routingKey, err := gocql.Marshal(
			partitionKeys[0].Type.CQLType(),
			values[0],
		)
		if err != nil {
			return nil, err
		}
		return routingKey, nil
	}

	if rc.routingKeyBuffer == nil {
		rc.routingKeyBuffer = make([]byte, 0, 256)
	}

	// composite routing key
	buf := bytes.NewBuffer(rc.routingKeyBuffer)
	for i := range partitionKeys {
		encoded, err := gocql.Marshal(
			partitionKeys[i].Type.CQLType(),
			values[i],
		)
		if err != nil {
			return nil, err
		}
		lenBuf := []byte{0x00, 0x00}
		binary.BigEndian.PutUint16(lenBuf, uint16(len(encoded)))
		buf.Write(lenBuf)
		buf.Write(encoded)
		buf.WriteByte(0x00)
	}
	routingKey := buf.Bytes()
	return routingKey, nil
}
