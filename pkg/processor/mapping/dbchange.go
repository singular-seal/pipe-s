package mapping

import (
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"strings"
)

type DBInfo struct {
	targetDB     string
	tableMapping map[string]*TableInfo
}

type TableInfo struct {
	targetTable   string
	actionMapping map[string]string
	columnMapping map[string]string
}

type DBChangeMappingProcessor struct {
	*core.BaseComponent
	dbNameVariable    string
	tableNameVariable string
	mappings          map[string]*DBInfo // source db -> target db info
}

func NewDBChangeMappingProcessor() *DBChangeMappingProcessor {
	return &DBChangeMappingProcessor{
		BaseComponent: core.NewBaseComponent(),
		mappings:      map[string]*DBInfo{},
	}
}

func (p *DBChangeMappingProcessor) Configure(config core.StringMap) (err error) {
	p.dbNameVariable, _ = utils.GetStringFromConfig(config, "$.DBNameVariable")
	p.tableNameVariable, _ = utils.GetStringFromConfig(config, "$.TableNameVariable")

	mappings, err := utils.GetConfigArrayFromConfig(config, "$.Mappings")
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		return errors.Errorf("no mapping config")
	}
	for _, mapping := range mappings {
		source, err := utils.GetConfigFromConfig(mapping, "$.Source")
		if err != nil {
			return err
		}
		target, err := utils.GetConfigFromConfig(mapping, "$.Target")
		if err != nil {
			return err
		}
		sourceDB, err := utils.GetStringFromConfig(source, "$.DB")
		if err != nil {
			return err
		}
		targetDB, err := utils.GetStringFromConfig(target, "$.DB")
		if err != nil {
			return err
		}
		dbInfo := &DBInfo{
			targetDB: targetDB,
		}
		p.mappings[sourceDB] = dbInfo

		configureTableMapping(dbInfo, source, target)
	}
	return nil
}

func configureTableMapping(dbInfo *DBInfo, source core.StringMap, target core.StringMap) error {
	sourceTables, err := utils.GetConfigArrayFromConfig(source, "$.Tables")
	if err != nil || len(sourceTables) == 0 {
		return nil
	}
	targetTables, err := utils.GetConfigArrayFromConfig(target, "$.Tables")
	if err != nil {
		return err
	}
	if len(sourceTables) != len(targetTables) {
		return errors.Errorf("source table count and target table count not equal")
	}
	dbInfo.tableMapping = map[string]*TableInfo{}
	for i, sourceTableMap := range sourceTables {
		targetTableMap := targetTables[i]
		sourceTable, err := utils.GetStringFromConfig(sourceTableMap, "$.Table")
		if err != nil {
			return err
		}
		targetTable, err := utils.GetStringFromConfig(targetTableMap, "$.Table")
		if err != nil {
			return err
		}
		tableInfo := &TableInfo{
			targetTable: targetTable,
		}
		dbInfo.tableMapping[sourceTable] = tableInfo

		if err = configureColumnMapping(tableInfo, sourceTableMap, targetTableMap); err != nil {
			return err
		}
		if err = configureActionMapping(tableInfo, sourceTableMap, targetTableMap); err != nil {
			return err
		}
	}
	return nil
}

func configureActionMapping(tableInfo *TableInfo, sourceTableMap core.StringMap, targetTableMap core.StringMap) error {
	sourceActions, err := utils.GetArrayFromConfig(sourceTableMap, "$.Actions")
	if err != nil || len(sourceActions) == 0 {
		return nil
	}
	targetActions, err := utils.GetArrayFromConfig(targetTableMap, "$.Actions")
	if err != nil {
		return err
	}
	if len(sourceActions) != len(targetActions) {
		return errors.Errorf("source action count and target action count not equal")
	}
	tableInfo.actionMapping = map[string]string{}
	for k, each := range sourceActions {
		sourceAction, ok := each.(string)
		if ok {
			return errors.Errorf("source action is not string")
		}
		targetAction, ok := targetActions[k].(string)
		if ok {
			return errors.Errorf("target action is not string")
		}
		tableInfo.actionMapping[sourceAction] = targetAction
	}
	return nil
}

func configureColumnMapping(tableInfo *TableInfo, sourceTableMap core.StringMap, targetTableMap core.StringMap) error {
	sourceColumns, err := utils.GetArrayFromConfig(sourceTableMap, "$.Columns")
	if err != nil || len(sourceColumns) == 0 {
		return nil
	}
	targetColumns, err := utils.GetArrayFromConfig(targetTableMap, "$.Columns")
	if err != nil {
		return err
	}
	if len(sourceColumns) != len(targetColumns) {
		return errors.Errorf("source column count and target column count not equal")
	}
	tableInfo.columnMapping = map[string]string{}
	for j, sourceColumnObj := range sourceColumns {
		targetColumnObj := targetColumns[j]
		sourceColumn, ok := sourceColumnObj.(string)
		if !ok {
			return errors.Errorf("source column is not string")
		}
		targetColumn, ok := targetColumnObj.(string)
		if !ok {
			return errors.Errorf("target column is not string")
		}
		tableInfo.columnMapping[sourceColumn] = targetColumn
	}
	return nil
}

func (p *DBChangeMappingProcessor) Process(msg *core.Message) (skip bool, err error) {
	event := msg.Body.(*core.DBChangeEvent)
	// mapping database name
	var dbInfo *DBInfo
	var ok bool
	if len(p.dbNameVariable) > 0 {
		obj, ok := msg.GetVariable(p.dbNameVariable)
		if !ok {
			return false, errors.Errorf("no DBNameVariable found: msg %s, db %s, table %s", msg.Header.ID,
				event.Database, event.Table)
		}
		logicalDB := obj.(string)
		if dbInfo, ok = p.mappings[logicalDB]; !ok {
			return
		}
		event.Database = strings.Replace(event.Database, logicalDB, dbInfo.targetDB, 1)
	} else {
		if dbInfo, ok = p.mappings[event.Database]; !ok {
			return
		}
		event.Database = dbInfo.targetDB
	}
	// mapping table name
	if dbInfo.tableMapping == nil {
		return
	}
	var tableInfo *TableInfo
	if len(p.tableNameVariable) > 0 {
		obj, ok := msg.GetVariable(p.tableNameVariable)
		if !ok {
			return false, errors.Errorf("no TableNameVariable found: msg %s, db %s, table %s", msg.Header.ID,
				event.Database, event.Table)
		}
		logicalTable := obj.(string)
		if tableInfo, ok = dbInfo.tableMapping[logicalTable]; !ok {
			return
		}
		event.Table = strings.Replace(event.Table, logicalTable, tableInfo.targetTable, 1)
	} else {
		if tableInfo, ok = dbInfo.tableMapping[event.Table]; !ok {
			return
		}
		event.Table = tableInfo.targetTable
	}

	if tableInfo.actionMapping != nil {
		processActionMapping(event, tableInfo.actionMapping)
	}
	if tableInfo.columnMapping != nil {
		processColumnMapping(event, tableInfo.columnMapping)
	}

	return
}

func processColumnMapping(event *core.DBChangeEvent, columnMapping map[string]string) {
	rows := make([]map[string]interface{}, 0)
	if event.NewRow != nil {
		rows = append(rows, event.NewRow)
	}
	if event.OldRow != nil {
		rows = append(rows, event.OldRow)
	}
	for _, row := range rows {
		for k, v := range row {
			if col, ok := columnMapping[k]; ok && col != k {
				row[col] = v
				delete(row, k)
			}
		}
	}
}

func processActionMapping(event *core.DBChangeEvent, actionMapping map[string]string) {
	if op, ok := actionMapping[event.Operation]; ok {
		switch event.Operation {
		case core.DBInsert:
			switch op {
			case core.DBUpdate:
				event.OldRow = event.NewRow
			case core.DBDelete:
				event.OldRow = event.NewRow
				event.NewRow = nil
			}
		case core.DBUpdate:
			switch op {
			case core.DBInsert:
				event.OldRow = nil
			case core.DBDelete:
				event.OldRow = event.NewRow
				event.NewRow = nil
			}
		case core.DBDelete:
			switch op {
			case core.DBInsert:
				event.NewRow = event.OldRow
				event.OldRow = nil
			case core.DBUpdate:
				event.NewRow = event.OldRow
			}
		}
		event.Operation = op
	}
}
