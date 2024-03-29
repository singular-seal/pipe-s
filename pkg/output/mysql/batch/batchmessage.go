package batch

import (
	"github.com/pkg/errors"
	"github.com/singular-seal/pipe-s/pkg/core"
)

type MergedMessage struct {
	mergedEvent *core.DBChangeEvent // the merged db change event
	originals   []*core.Message     // originals keep tracking the original messages before merge
	inDB        bool                // inDB indicates if the row should be in the db before the MergedMessage received.
}

type BatchMessage struct {
	size           int
	mergedMessages map[interface{}]*MergedMessage
}

func NewBatchMessage() *BatchMessage {
	return &BatchMessage{
		mergedMessages: map[interface{}]*MergedMessage{},
	}
}

func (bm *BatchMessage) add(info *MessageInfo) (err error) {
	bm.size++
	oldMessage, ok := bm.mergedMessages[*info.key]
	if !ok {
		bm.mergedMessages[*info.key] = &MergedMessage{
			mergedEvent: info.dbChange,
			originals:   []*core.Message{info.message},
			// if new event is insert then the pk shouldn't be in db before this event
			inDB: info.dbChange.Operation != core.DBInsert,
		}
		return
	}
	oldMessage.originals = append(oldMessage.originals, info.message)

	switch info.dbChange.Operation {
	case core.DBDelete:
		oldMessage.mergedEvent = info.dbChange
		return
	case core.DBInsert:
		if oldMessage.mergedEvent.Operation != core.DBDelete {
			return errors.Errorf("insert can only be preceded by delete:%s", oldMessage.mergedEvent.ID)
		}
		mergeInsert(oldMessage, info)
		return
	case core.DBUpdate:
		if oldMessage.mergedEvent.Operation == core.DBDelete {
			return errors.Errorf("update can't be preceded by delete:%s", oldMessage.mergedEvent.ID)
		}
		mergeUpdate(oldMessage.mergedEvent, info.dbChange)
		return
	}
	return
}

// mergeInsert for delete-insert sequence
func mergeInsert(oldEvent *MergedMessage, newEvent *MessageInfo) {
	// not in db before, so we can just use make a entire new event
	// like insert->*->delete->insert
	if !oldEvent.inDB {
		oldEvent.mergedEvent = newEvent.dbChange
		return
	}

	// change to update
	// like *->delete->insert
	oldEvent.mergedEvent.Operation = core.DBUpdate
	oldEvent.mergedEvent.NewRow = make(map[string]interface{})
	for col := range oldEvent.mergedEvent.OldRow {
		value, ok := newEvent.dbChange.NewRow[col]
		if ok {
			oldEvent.mergedEvent.NewRow[col] = value
		} else {
			// use nil to represent default value
			oldEvent.mergedEvent.NewRow[col] = nil
		}
	}
}

// mergeUpdate for insert-update or update-update sequence
func mergeUpdate(oldEvent *core.DBChangeEvent, newEvent *core.DBChangeEvent) {
	for colName, value := range newEvent.NewRow {
		oldEvent.NewRow[colName] = value
	}
}

// splitByOperation split messages to insert, updates, deletes and replaces batches
func (bm *BatchMessage) splitByOperation() (batches [][]*MergedMessage) {
	inserts, updates, deletes := make([]*MergedMessage, 0), make([]*MergedMessage, 0), make([]*MergedMessage, 0)
	for _, each := range bm.mergedMessages {
		switch each.mergedEvent.Operation {
		case core.DBInsert:
			inserts = append(inserts, each)
		case core.DBUpdate:
			updates = append(updates, each)
		case core.DBDelete:
			deletes = append(deletes, each)
		}
	}
	batches = [][]*MergedMessage{inserts, updates, deletes}
	return
}
