package filter

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/utils"
	"regexp"
)

type MysqlDMLFilterConfig struct {
	ID                   string
	FullTableNamePattern string
	Operations           []string
}

// MysqlDMLFilter filter dml events by full table name and operations. We don't filter columns here
// is because we don't have column names in dml events so have to postpone it to when we have db change event.
type MysqlDMLFilter struct {
	*core.BaseComponent
	tableRegex *regexp.Regexp
	operations map[string]bool
}

func NewMysqlDMLFilter() *MysqlDMLFilter {
	return &MysqlDMLFilter{
		BaseComponent: core.NewBaseComponent(),
		operations:    map[string]bool{},
	}
}

func (f *MysqlDMLFilter) Configure(config core.StringMap) (err error) {
	c := &MysqlDMLFilterConfig{}
	if err = utils.ConfigToStruct(config, c); err != nil {
		return
	}
	f.ID = c.ID

	if len(c.FullTableNamePattern) > 0 {
		if f.tableRegex, err = regexp.Compile(c.FullTableNamePattern); err != nil {
			return
		}
	}
	if len(c.Operations) > 0 {
		for _, each := range c.Operations {
			f.operations[each] = true
		}
	}
	return nil
}

func (f *MysqlDMLFilter) Process(msg *core.Message) (bool, error) {
	event := msg.Body.(*core.MysqlDMLEvent)
	if f.tableRegex != nil {
		if !f.tableRegex.MatchString(event.FullTableName) {
			return true, nil
		}
	}
	if len(f.operations) > 0 {
		if _, ok := f.operations[event.Operation]; !ok {
			return true, nil
		}
	}
	return false, nil
}
