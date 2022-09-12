package converter

import (
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type MysqlDMLToDBChangeConverter struct {
	*core.BaseComponent
	config *MysqlDMLToDBChangeConfig
}

type MysqlDMLToDBChangeConfig struct {
	ID string
}

func NewMysqlDMLToDBChangeConverter() *MysqlDMLToDBChangeConverter {
	return &MysqlDMLToDBChangeConverter{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (c *MysqlDMLToDBChangeConverter) Configure(config core.StringMap) (err error) {
	co := &MysqlDMLToDBChangeConfig{}
	err = utils.ConfigToStruct(config, co)
	return err
}

func (c *MysqlDMLToDBChangeConverter) Process(msg *core.Message) (bool, error) {
	dml := msg.Data.(*core.MysqlDMLEvent)
	dbChange := &core.DBChangeEvent{
		ExtraInfo: make(map[string]interface{}),
	}
	ts, ok := msg.GetTableSchema()
	if !ok {
		return false, core.NoSchemaError("", dml.FullTableName)
	}

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
	msg.Type = core.TypeDBChange
	return false, nil
}

func (c *MysqlDMLToDBChangeConverter) convertRow(row []interface{}, ts *core.Table) (map[string]interface{}, error) {
	if len(row) != len(ts.Columns) {
		return nil, errors.Errorf("col count mismatch - schema:%d binlog:%d", len(ts.Columns), len(row))
	}
	result := make(map[string]interface{})
	for i, col := range ts.Columns {
		result[col.Name] = row[i]
	}
	return result, nil
}
