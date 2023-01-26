package converter

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

const (
	RoutingByTable      = "Table"
	RoutingByPrimaryKey = "PrimaryKey"
)

type DBChangeToSQLConverter struct {
	*core.BaseComponent
	config *DBChangeToSQLConverterConfig
}

type DBChangeToSQLConverterConfig struct {
	RoutingBy string
}

func NewDBChangeToSQLConverter() *DBChangeToSQLConverter {
	return &DBChangeToSQLConverter{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (c *DBChangeToSQLConverter) Configure(config core.StringMap) (err error) {
	co := &DBChangeToSQLConverterConfig{}
	if err = utils.ConfigToStruct(config, co); err != nil {
		return
	}
	c.config = co
	if len(c.config.RoutingBy) == 0 {
		c.config.RoutingBy = RoutingByTable
	}
	return
}

func (c *DBChangeToSQLConverter) Process(msg *core.Message) (bool, error) {
	dbChange := msg.Data.(*core.DBChangeEvent)
	sqlEvent := &core.SQLEvent{
		Database:  dbChange.Database,
		Table:     dbChange.Table,
		Operation: dbChange.Operation,
	}
	msg.Data = sqlEvent
	msg.Type = core.TypeSQData
	ts, ok := msg.GetTableSchema()
	if !ok {
		return false, core.NoSchemaError(dbChange.Database, dbChange.Table)
	}
	pkCols := ts.PKColumnNames()
	if len(pkCols) == 0 {
		return false, core.NoPKError(dbChange.Database, dbChange.Table)
	}

	sqlEvent.SQLString, sqlEvent.SQLArgs = utils.GenerateSqlAndArgs(dbChange, pkCols)
	if c.config.RoutingBy == RoutingByPrimaryKey {
		msg.SetMeta(core.RoutingKey, genKeyHash(dbChange.Database, dbChange.Table, getKeyValues(dbChange, pkCols)))
	} else {
		msg.SetMeta(core.RoutingKey, genTableHash(dbChange.Database, dbChange.Table))
	}

	return false, nil
}

func getKeyValues(event *core.DBChangeEvent, cols []string) []interface{} {
	result := make([]interface{}, 0, len(cols))
	for _, col := range cols {
		result = append(result, event.GetRow()[col])
	}
	return result
}

func genKeyHash(db string, table string, pks []interface{}) int {
	result := utils.GetFNV64aHash(db)
	result += utils.GetFNV64aHash(table)
	for _, pk := range pks {
		result += utils.GetFNV64aHash(fmt.Sprintf("%v", pk))
	}
	return utils.AbsInt(result)
}

func genTableHash(db string, table string) int {
	result := utils.GetFNV64aHash(db)
	result += utils.GetFNV64aHash(table)
	return utils.AbsInt(result)
}
