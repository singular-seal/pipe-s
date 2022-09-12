package core

import (
	"encoding/json"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	TypeDML      = "dml"
	TypeJsonByte = "json_byte"
)

type MessageHeader struct {
	ID         string              // ID of the message
	CreateTime uint64              // Unix timestamp in nanoseconds
	MetaMap    map[int]interface{} // Other meta data
}

// Message is the basic data container which is transferred throughout the whole system.
type Message struct {
	Type   string
	Header *MessageHeader
	Data   interface{}
}

// SetMeta set meta value.
func (m *Message) SetMeta(id int, data interface{}) {
	m.Header.MetaMap[id] = data
}

// GetMeta get meta value.
func (m *Message) GetMeta(id int) (interface{}, bool) {
	data, ok := m.Header.MetaMap[id]
	return data, ok
}

func NewMessage(typeName string) *Message {
	return &Message{
		Header: &MessageHeader{
			MetaMap: make(map[int]interface{}),
		},
		Type: typeName,
	}
}

const (
	MetaUndefined int = iota
	CustomVariable
	MetaMySQLPos
	AckWaitGroup
)

// db operation types
const (
	DBInsert = "insert"
	DBUpdate = "update"
	DBDelete = "delete"
)

// MysqlBinlogPosition describes position in mysql binlog.
type MysqlBinlogPosition struct {
	BinlogName        string // binlog filename
	BinlogPos         uint32 // binlog position
	Timestamp         uint32 // binlog timestamp
	ServerID          uint32 // mysql server_id
	ServerUUID        string // server UUID
	TransactionID     int64  // transaction ID
	FullGTIDSetString string // full GTID as string (for serialization)
	RowOffset         int    // offset of the row in current batch
	TransactionOffset int    //offset in a transaction

	FullGTIDSet mysql.GTIDSet `json:"-"` // parsed GTID set (for runtime)
}

// SimpleCopy clones a new position instance but skip FullGTIDSet which costs much.
func (p *MysqlBinlogPosition) SimpleCopy() *MysqlBinlogPosition {
	return &MysqlBinlogPosition{
		BinlogName:        p.BinlogName,
		BinlogPos:         p.BinlogPos,
		Timestamp:         p.Timestamp,
		ServerID:          p.ServerID,
		ServerUUID:        p.ServerUUID,
		TransactionID:     p.TransactionID,
		FullGTIDSetString: p.FullGTIDSetString,
		RowOffset:         p.RowOffset,
		TransactionOffset: p.TransactionOffset,
	}
}

func MarshalMysqlBinlogPosition(p *MysqlBinlogPosition) ([]byte, error) {
	/*	if p.FullGTIDSet != nil {
		p.FullGTIDSetString = p.FullGTIDSet.String()
	}*/
	// we carry gtid set by string in messages, so needn't use FullGTIDSet
	if data, err := json.Marshal(p); err == nil {
		return data, nil
	} else {
		return nil, err
	}
}

func UnmarshalMysqlBinlogPosition(p *MysqlBinlogPosition, data []byte) (err error) {
	if err = json.Unmarshal(data, p); err != nil {
		return err
	}
	if p.FullGTIDSet, err = mysql.ParseMysqlGTIDSet(p.FullGTIDSetString); err != nil {
		return err
	}
	return
}

// MysqlDMLEvent describes mysql event
type MysqlDMLEvent struct {
	Pos      *MysqlBinlogPosition     // replication position
	RawEvent *replication.BinlogEvent // binlog event from go mysql
	Table    string                   // mysql table  name
	OPType   string                   // mysql operation type
	OldRow   []interface{}            // old DB row values
	NewRow   []interface{}            // new DB row values
}
