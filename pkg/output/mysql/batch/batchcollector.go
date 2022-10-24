package batch

import (
	"database/sql"
	"fmt"
	"git.garena.com/common/gommon/logger"
	"git.garena.com/shopee/platform/suez/pkg/core"
	"git.garena.com/shopee/platform/suez/pkg/utils"
	"strings"
)

type Data struct {
	sqlCommand *core.SQLCommand
	toAck      []*core.MessageMeta
}

// map from primary key to sql command
type batchOfData map[interface{}]*Data

// BatchCollector is responsible for collecting a group of sql commands to an appropriate size, then sending to the db
type BatchCollector struct {
	processor    *LongMysqlOutStreamProcessor
	cmd          string
	batchSize    int
	allBatches   []batchOfData
	currentBatch batchOfData
	dbClient     *sql.DB
	logger       logger.ILogger
}

func NewBatchCollector(proc *LongMysqlOutStreamProcessor, cmd string) *BatchCollector {
	return &BatchCollector{
		processor:    proc,
		cmd:          cmd,
		batchSize:    proc.outstream.config.SendBatchSize,
		currentBatch: batchOfData{},
		allBatches:   make([]batchOfData, 0),
		dbClient:     proc.outstream.dbClient,
		logger:       proc.outstream.logger,
	}
}

func (s *BatchCollector) collateCurrentBatch() {
	s.allBatches = append(s.allBatches, s.currentBatch)
	s.currentBatch = batchOfData{}
}

func (s *BatchCollector) AddMessage(pk interface{}, sqlCommand *core.SQLCommand, toAck []*core.MessageMeta) {
	s.currentBatch[pk] = &Data{
		sqlCommand: sqlCommand,
		toAck:      toAck,
	}

	if len(s.currentBatch) == s.batchSize {
		s.collateCurrentBatch()
	}
}

func (s *BatchCollector) Flush() {
	if len(s.currentBatch) == 0 && len(s.allBatches) == 0 {
		return
	}

	if len(s.currentBatch) > 0 {
		s.collateCurrentBatch()
	}

	for _, batch := range s.allBatches {
		s.execBatch(batch)
	}
	s.allBatches = make([]batchOfData, 0)
}

func (s *BatchCollector) execBatch(batch batchOfData) {
	s.logger.WithField("batch_sql_size", len(batch)).Debug("sql_size")
	sqlCmd, sqlArgs := s.generateCombinedSqlCommand(batch)
	err := s.dbExecBatch(sqlCmd, sqlArgs, len(batch))
	if err != nil {
		s.logger.WithError(err).Error("db_execute_error")
	}

	for _, data := range batch {
		s.processor.helpAck(data.toAck, err)
	}
}

func (s *BatchCollector) dbExecBatch(sql string, args []interface{}, rowCount int) error {
	result, err := s.dbClient.Exec(sql, args...)
	if err != nil {
		return err
	}
	realCount, err := result.RowsAffected()
	if realCount < int64(rowCount) {
		s.logger.Warn(fmt.Sprintf("not_all_rows_completed:%d|%d", realCount, rowCount))
	}
	return err
}

func (s *BatchCollector) generateCombinedSqlCommand(batch batchOfData) (sqlCmd string, sqlArgs []interface{}) {
	switch s.cmd {
	case core.MySQLCommandInsert:
		return generateSqlInsert(batch)
	case core.MySQLCommandUpdate:
		return generateSqlUpdate(batch)
	case core.MySQLCommandDelete:
		return generateSqlDelete(batch)
	default:
		return "", nil
	}
}

func generateSqlInsert(batch batchOfData) (sqlCmd string, sqlArgs []interface{}) {
	var msg0 *core.SQLCommand
	for _, msg := range batch {
		msg0 = msg.sqlCommand
		break
	}

	columns := msg0.ColumnNames()
	sqlPrefix := fmt.Sprintf("INSERT IGNORE INTO `%s`.`%s` (%s) VALUES", msg0.Database, msg0.Table, strings.Join(utils.QuoteColumnNames(columns), ","))
	batchPlaceHolders := make([]string, 0)
	batchArgs := make([]interface{}, 0)

	for _, data := range batch {
		for _, arg := range columnValues(data.sqlCommand, columns) {
			batchArgs = append(batchArgs, arg)
		}
		placeHolders := make([]string, 0)
		for i := 0; i < len(columns); i++ {
			placeHolders = append(placeHolders, "?")
		}
		singleSqlPlaceHolder := fmt.Sprintf("(%s)", strings.Join(placeHolders, ","))
		batchPlaceHolders = append(batchPlaceHolders, singleSqlPlaceHolder)
	}

	finalPlaceHolders := strings.Join(batchPlaceHolders, ",")
	s := []string{sqlPrefix, finalPlaceHolders}
	return strings.Join(s, " "), batchArgs
}

func generateSqlUpdate(batch batchOfData) (sqlCmd string, sqlArgs []interface{}) {
	var sqlCmdBuilder []string
	for _, data := range batch {
		sql := data.sqlCommand
		utils.GenerateSql(sql)
		sqlCmdBuilder = append(sqlCmdBuilder, sql.SQLString)
		sqlArgs = append(sqlArgs, sql.SQLArgs...)
	}

	return strings.Join(sqlCmdBuilder, ";"), sqlArgs
}

func generateSqlDelete(batch batchOfData) (sqlCmd string, sqlArgs []interface{}) {
	var msg0 *core.SQLCommand
	for _, data := range batch {
		msg0 = data.sqlCommand
		break
	}

	keyColumns := msg0.KeyColumnNames()
	sqlPrefix := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE (%s) IN", msg0.Database, msg0.Table, strings.Join(keyColumns, ", "))
	batchPlaceHolders := make([]string, 0)
	batchArgs := make([]interface{}, 0)

	for _, data := range batch {
		for _, arg := range columnValues(data.sqlCommand, keyColumns) {
			batchArgs = append(batchArgs, arg)
		}
		placeHolders := make([]string, 0)
		for i := 0; i < len(keyColumns); i++ {
			placeHolders = append(placeHolders, "?")
		}
		singleSqlPlaceHolder := fmt.Sprintf("(%s)", strings.Join(placeHolders, ","))
		batchPlaceHolders = append(batchPlaceHolders, singleSqlPlaceHolder)
	}

	finalPlaceHolders := fmt.Sprintf("( %s )", strings.Join(batchPlaceHolders, ","))
	s := []string{sqlPrefix, finalPlaceHolders}
	return strings.Join(s, " "), batchArgs
}
