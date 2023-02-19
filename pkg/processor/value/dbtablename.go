package value

import (
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"regexp"
	"strings"
)

type DBTableNameCatcherConfig struct {
	// if TableNamePattern and TableNameVariable both configured, we will use the sub match as table name
	// or only TableNameVariable configured, we will use table name of db event directly
	TableNamePattern  string
	TableNameVariable string
	// if DBNamePattern and DBNameVariable both configured, we will use the sub match as db name
	// or only DBNameVariable configured, we will use db name of db event directly
	DBNamePattern  string
	DBNameVariable string
}

type DBTableNameCatcher struct {
	*core.BaseComponent
	config           *DBTableNameCatcherConfig
	dbNamePattern    *regexp.Regexp
	tableNamePattern *regexp.Regexp
}

func NewLogicalNameCatcher() *DBTableNameCatcher {
	return &DBTableNameCatcher{
		BaseComponent: core.NewBaseComponent(),
	}
}

func (catcher *DBTableNameCatcher) Configure(config core.StringMap) (err error) {
	c := &DBTableNameCatcherConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	catcher.config = c
	if len(c.DBNameVariable) == 0 && len(c.TableNameVariable) == 0 {
		return errors.New("db pattern and table pattern are both blank")
	}
	if len(c.DBNamePattern) > 0 && len(c.DBNameVariable) == 0 || len(c.TableNamePattern) > 0 && len(c.TableNameVariable) == 0 {
		return errors.New("variable not configured")
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

func (catcher *DBTableNameCatcher) Process(msg *core.Message) (bool, error) {
	event := msg.Data.(*core.MysqlDMLEvent)
	dt := strings.Split(event.FullTableName, ".")
	if len(dt) != 2 {
		return false, errors.Errorf("illegal full table name:%s, msg id:%s", event.FullTableName, msg.Header.ID)
	}
	db, table := dt[0], dt[1]
	if catcher.dbNamePattern != nil {
		matches := catcher.dbNamePattern.FindStringSubmatch(db)
		if len(matches) > 1 {
			msg.SetVariable(catcher.config.DBNameVariable, matches[1])
		}
	} else if len(catcher.config.DBNameVariable) > 0 {
		msg.SetVariable(catcher.config.DBNameVariable, db)
	}

	if catcher.tableNamePattern != nil {
		matches := catcher.tableNamePattern.FindStringSubmatch(table)
		if len(matches) > 1 {
			msg.SetVariable(catcher.config.TableNameVariable, matches[1])
		}
	} else if len(catcher.config.TableNameVariable) > 0 {
		msg.SetVariable(catcher.config.TableNameVariable, table)
	}
	return false, nil
}
