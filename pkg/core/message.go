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

func (m *Message) GetTableSchema() (*Table, bool) {
	obj, ok := m.GetMeta(MetaTableSchema)
	if !ok {
		return nil, false
	}
	ts, ok := obj.(*Table)
	return ts, ok
}

func (m *Message) ColumnNames() []string {
	ts, ok := m.GetTableSchema()
	if !ok {
		return nil
	}
	return ts.ColumnNames()
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
	MetaTableSchema
	AckWaitGroup
)

// db operation types
const (
	DBInsert  = "insert"
	DBUpdate  = "update"
	DBDelete  = "delete"
	DBReplace = "replace" // we generate replace event during processing
)

// MysqlBinlogPosition describes position in mysql binlog.
type MysqlBinlogPosition struct {
	BinlogName        string // binlog filename
	BinlogPos         uint32 // binlog position
	TxBinlogPos       uint32 // last committed transaction binlog position
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
		TxBinlogPos:       p.TxBinlogPos,
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

// MysqlDMLEvent describes mysql binlog event
type MysqlDMLEvent struct {
	Pos           *MysqlBinlogPosition     // replication position
	BinlogEvent   *replication.BinlogEvent // binlog event from go mysql
	FullTableName string                   // mysql full table  name - db.table
	Operation     string                   // mysql operation type
	OldRow        []interface{}            // old DB row values
	NewRow        []interface{}            // new DB row values
}

// DBChangeEvent is the standard object describes a database row change
type DBChangeEvent struct {
	ID        string                 // id
	Database  string                 // database name
	Table     string                 // table name
	DBTime    uint64                 // binlog time
	EventTime uint64                 // time event is created
	Operation string                 // insert/update/delete
	OldRow    map[string]interface{} // DB row values before change
	NewRow    map[string]interface{} // DB row values after change
	ExtraInfo map[string]interface{} // can put everything else here
}

func (e *DBChangeEvent) GetRow() map[string]interface{} {
	if e.Operation == DBDelete {
		return e.OldRow
	}
	return e.NewRow
}
