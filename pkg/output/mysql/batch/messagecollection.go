package batch

import (
	"github.com/singular-seal/pipe-s/pkg/core"
	"sync"
)

type ProcessorMessage struct {
	// dbEvent is the merged event
	dbEvent *core.DBChangeEvent
	// toAck contains all the messages metas of the merged db event. This data needs to be preserved until the db result is returned
	toAck []*core.MessageMeta
	// existsInDb indicates if the row exists in the db
	existsInDb bool
}

type EventMap map[interface{}]*ProcessorMessage

type MessageCollection struct {
	mutex  *sync.Mutex
	events EventMap
}

func NewMessageCollection() *MessageCollection {
	return &MessageCollection{
		mutex:  &sync.Mutex{},
		events: EventMap{},
	}
}

func (c *MessageCollection) getSnapshot() (toProcess EventMap) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	toProcess = c.events
	c.events = EventMap{}
	return toProcess
}

func (c *MessageCollection) addToBatch(pk interface{}, newEvent *core.DBChangeEvent, meta *core.MessageMeta) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	oldMessage, ok := c.events[pk]
	if !ok {
		// no merging needed
		c.events[pk] = &ProcessorMessage{
			dbEvent:    newEvent,
			toAck:      []*core.MessageMeta{meta},
			existsInDb: newEvent.Command != core.MySQLCommandInsert,
		}
		return
	}

	// add ack
	oldMessage.toAck = append(oldMessage.toAck, meta)

	switch newEvent.Command {

	case core.MySQLCommandDelete:
		oldMessage.dbEvent = newEvent
		return
	case core.MySQLCommandInsert:
		// insert can only be preceded by DELETE
		if oldMessage.dbEvent.Command != core.MySQLCommandDelete {
			return
		}
		oldMessage.dbEvent = mergeNewInsert(oldMessage.dbEvent, newEvent, oldMessage.existsInDb)
		return
	case core.MySQLCommandUpdate:
		// update can be preceded by INSERT or UPDATE
		if oldMessage.dbEvent.Command == core.MySQLCommandDelete {
			return
		}
		mergeNewUpdate(oldMessage.dbEvent, newEvent)
		return
	}
}

// mergeNewInsert is for DELETE-INSERT sequence
// needs a return value due to the case of changing pointer (!existsInDb)
func mergeNewInsert(oldEvent *core.DBChangeEvent, newEvent *core.DBChangeEvent, existsInDb bool) *core.DBChangeEvent {
	// the command should be UPDATE/ INSERT depending on whether the data exists in db
	if !existsInDb {
		return newEvent
	}

	// for UPDATE, some values have to be set back to DEFAULT
	oldEvent.Command = core.MySQLCommandUpdate
	if oldEvent.NewRow == nil {
		oldEvent.NewRow = make(map[string]interface{})
	}
	for colName := range oldEvent.OldRow {
		newValue, ok := newEvent.NewRow[colName]
		if ok {
			oldEvent.NewRow[colName] = newValue
		} else {
			oldEvent.NewRow[colName] = "DEFAULT"
		}
	}

	return oldEvent
}

// mergeNewUpdate is for INSERT-UPDATE or UPDATE-UPDATE sequence
func mergeNewUpdate(oldEvent *core.DBChangeEvent, newEvent *core.DBChangeEvent) {
	for colName, value := range newEvent.NewRow {
		oldEvent.NewRow[colName] = value
	}
}
