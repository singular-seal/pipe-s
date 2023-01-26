package converter

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type DBChangeToSQLConverter struct {
	*core.BaseComponent
}

func NewDBChangeToSQLConverter() *DBChangeToSQLConverter {
	return &DBChangeToSQLConverter{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (c *DBChangeToSQLConverter) Configure(config core.StringMap) (err error) {
	return nil
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
	msg.SetMeta(core.RoutingKey, genKeyHash(dbChange.Database, dbChange.Table, getKeyValues(dbChange, pkCols)))
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
	return result
}
