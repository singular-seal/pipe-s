package value

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"regexp"
	"strings"
)

type LogicalNameCatcherConfig struct {
	TableNamePattern  string
	TableNameVariable string
	DBNamePattern     string
	DBNameVariable    string
}

type LogicalNameCatcher struct {
	*core.BaseComponent
	config           *LogicalNameCatcherConfig
	dbNamePattern    *regexp.Regexp
	tableNamePattern *regexp.Regexp
}

func NewLogicalNameCatcher() *LogicalNameCatcher {
	return &LogicalNameCatcher{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (catcher *LogicalNameCatcher) Configure(config core.StringMap) (err error) {
	c := &LogicalNameCatcherConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	catcher.config = c
	if len(c.DBNameVariable) == 0 && len(c.TableNameVariable) == 0 {
		return fmt.Errorf("db pattern and table pattern are both blank")
	}
	if len(c.DBNamePattern) > 0 && len(c.DBNameVariable) == 0 || len(c.TableNamePattern) > 0 && len(c.TableNameVariable) == 0 {
		return fmt.Errorf("variable not configured")
	}

	if len(c.DBNamePattern) > 0 {
		if catcher.dbNamePattern, err = regexp.Compile(c.DBNamePattern); err != nil {
			return
		}
	}
	if len(c.TableNamePattern) > 0 {
		if catcher.tableNamePattern, err = regexp.Compile(c.TableNamePattern); err != nil {
			return
		}
	}

	return nil
}

func (catcher *LogicalNameCatcher) Process(msg *core.Message) (bool, error) {
	event := msg.Data.(*core.MysqlDMLEvent)
	dt := strings.Split(event.FullTableName, ".")
	if len(dt) != 2 {
		return false, fmt.Errorf("illegal full table name:%s, msg id:%s", event.FullTableName, msg.Header.ID)
	}
	db, table := dt[0], dt[1]
	if catcher.dbNamePattern != nil {
		matches := catcher.dbNamePattern.FindStringSubmatch(db)
		if len(matches) > 1 {
			msg.SetVariable(catcher.config.DBNameVariable, matches[1])
		}
	}
	if catcher.tableNamePattern != nil {
		matches := catcher.tableNamePattern.FindStringSubmatch(table)
		if len(matches) > 1 {
			msg.SetVariable(catcher.config.TableNameVariable, matches[1])
		}
	}
	return false, nil
}
