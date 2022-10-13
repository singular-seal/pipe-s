package converter

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/schema"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type MysqlDMLToDBChangeConverter struct {
	*core.BaseComponent
	config *MysqlDMLToDBChangeConverterConfig
}

type MysqlDMLToDBChangeConverterConfig struct {
	ID string
}

func NewMysqlDMLToDBChangeConverter() *MysqlDMLToDBChangeConverter {
	return &MysqlDMLToDBChangeConverter{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (c *MysqlDMLToDBChangeConverter) Configure(config core.StringMap) (err error) {
	co := &MysqlDMLToDBChangeConverterConfig{}
	err = utils.ConfigToStruct(config, co)
	return err
}

func (c *MysqlDMLToDBChangeConverter) Process(msg *core.Message) (bool, error) {
	dml := msg.Data.(*core.MysqlDMLEvent)
	dbChange := &core.DBChangeEvent{
		ExtraInfo: make(map[string]interface{}),
	}
	obj, ok := msg.GetMeta(core.MetaTableSchema)
	if !ok {
		return false, fmt.Errorf("no table schema")
	}
	ts := obj.(*schema.Table)

	dbChange.Database = ts.DBName
	dbChange.Table = ts.TableName
	if dml.BinlogEvent != nil {
		dbChange.DBTime = uint64(dml.BinlogEvent.Header.Timestamp)
	}
	dbChange.EventTime = msg.Header.CreateTime
	dbChange.Operation = dml.Operation
	dbChange.ID = msg.Header.ID

	var err error
	if len(dml.OldRow) > 0 {
		if dbChange.OldRow, err = c.convertRow(dml.OldRow, ts); err != nil {
			return false, err
		}
	}
	if len(dml.NewRow) > 0 {
		if dbChange.NewRow, err = c.convertRow(dml.NewRow, ts); err != nil {
			return false, err
		}
	}
	msg.Data = dbChange
	return false, nil
}

func (c *MysqlDMLToDBChangeConverter) convertRow(row []interface{}, ts *schema.Table) (map[string]interface{}, error) {
	if len(row) != len(ts.Columns) {
		return nil, fmt.Errorf("col count mismatch - schema:%d binlog:%d", len(ts.Columns), len(row))
	}
	result := make(map[string]interface{})
	for i, col := range ts.Columns {
		result[col.Name] = row[i]
	}
	return result, nil
}