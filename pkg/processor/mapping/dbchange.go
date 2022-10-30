package mapping

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

type DBInfo struct {
	targetDB     string
	tableMapping map[string]*TableInfo
}

type TableInfo struct {
	targetTable   string
	actionMapping map[string]string
	columnMapping map[string]*string
}

type DBChangeMappingProcessor struct {
	*core.BaseComponent
	mappings map[string]*DBInfo // source db -> target db info
}

func NewDBChangeMappingProcessor() *DBChangeMappingProcessor {
	return &DBChangeMappingProcessor{
		BaseComponent: core.NewBaseComponent(),
		mappings:      map[string]*DBInfo{},
	}
}

func (p *DBChangeMappingProcessor) Configure(config core.StringMap) error {
	mappings, err := utils.GetConfigArrayFromConfig(config, "$.Mappings")
	if err != nil {
		return err
	}
	if len(mappings) == 0 {
		return fmt.Errorf("no mapping config")
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
		// parse table mapping
		sourceTables, err := utils.GetConfigArrayFromConfig(source, "$.Tables")
		if err != nil || len(sourceTables) == 0 {
			continue
		}
		targetTables, err := utils.GetConfigArrayFromConfig(target, "$.Tables")
		if err != nil {
			return err
		}
		if len(sourceTables) != len(targetTables) {
			return fmt.Errorf("source table count and target table count not equal")
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
			// parse column mapping
			sourceColumns, err := utils.GetConfigArrayFromConfig(sourceTableMap, "$.Columns")
			if err != nil || len(sourceColumns) == 0 {
				continue
			}
			targetColumns, err := utils.GetConfigArrayFromConfig(targetTableMap, "$.Columns")
			if err != nil {
				return err
			}
			if len(sourceColumns) != len(targetColumns) {
				return fmt.Errorf("source column count and target column count not equal")
			}
			tableInfo.columnMapping = map[string]*ColumnInfo{}
			for j, sourceColumnMap := range sourceColumns {
				targetColumnMap := targetColumns[j]
				sourceColumn, err := utils.GetStringFromConfig(sourceColumnMap, "$.Column")
				if err != nil {
					return err
				}
				targetColumn, err := utils.GetStringFromConfig(targetColumnMap, "$.Column")
				if err != nil {
					return err
				}
				columnInfo := &ColumnInfo{
					targetColumn: targetColumn,
				}
				tableInfo.columnMapping[sourceColumn] = columnInfo
				// parse action mapping
				sourceActions, err := utils.GetArrayFromConfig(sourceColumnMap, "$.Actions")
				if err != nil || len(sourceActions) == 0 {
					continue
				}
				targetActions, err := utils.GetArrayFromConfig(targetColumnMap, "$.Actions")
				if err != nil {
					return err
				}
				if len(sourceActions) != len(targetActions) {
					return fmt.Errorf("source action count and target action count not equal")
				}
				columnInfo.actionMapping = map[string]string{}
				for k, each := range sourceActions {
					sourceAction, ok := each.(string)
					if ok {
						return fmt.Errorf("source action is not string")
					}
					targetAction, ok := targetActions[k].(string)
					if ok {
						return fmt.Errorf("target action is not string")
					}
					columnInfo.actionMapping[sourceAction] = targetAction
				}
			}
		}
	}
	return nil
}

func (p *DBChangeMappingProcessor) Process(msg *core.Message) (skip bool, err error) {
	event := msg.Data.(*core.DBChangeEvent)
	dbInfo, ok := p.mappings[event.Database]
	if !ok {
		return
	}
	event.Database = dbInfo.targetDB
	if dbInfo.tableMapping == nil {
		return
	}
	tableInfo, ok := dbInfo.tableMapping[event.Table]
	if !ok {
		return
	}
	event.Table = tableInfo.targetTable
	if tableInfo.columnMapping == nil {
		return
	}
	event.GetRow()
	return
}
