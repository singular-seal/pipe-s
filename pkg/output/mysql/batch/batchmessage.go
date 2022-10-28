package batch

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
)

type MergedMessage struct {
	mergedEvent *core.DBChangeEvent // the merged db change event
	originals   []*core.Message     // originals keep tracking the original messages before merge
	inDB        bool                // inDB indicates if the row inDB in the db
}

type BatchMessage struct {
	merged         bool
	originalSize   int
	mergedMessages map[interface{}]*MergedMessage // for normal crud scenario
	messages       []*core.Message                // for insert only scenario
}

func NewBatchMessage(merged bool) *BatchMessage {
	if merged {
		return &BatchMessage{
			mergedMessages: map[interface{}]*MergedMessage{},
			merged:         merged,
		}
	} else {
		return &BatchMessage{
			messages: []*core.Message{},
			merged:   merged,
		}
	}
}

func (bm *BatchMessage) size() int {
	if bm.merged {
		return bm.originalSize
	} else {
		return len(bm.messages)
	}
}

func (bm *BatchMessage) add(info *MessageInfo) {
	bm.messages = append(bm.messages, info.message)
}

func (bm *BatchMessage) merge(info *MessageInfo) (err error) {
	bm.originalSize++
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
			return fmt.Errorf("insert can only be preceded by delete:%s", oldMessage.mergedEvent.ID)
		}
		mergeInsert(oldMessage, info)
		return
	case core.DBUpdate:
		if oldMessage.mergedEvent.Operation == core.DBDelete {
			return fmt.Errorf("update can't be preceded by delete:%s", oldMessage.mergedEvent.ID)
		}
		mergeUpdate(oldMessage.mergedEvent, info.dbChange)
		return
	}
	return
}

// mergeInsert for delete-insert sequence
func mergeInsert(oldEvent *MergedMessage, newEvent *MessageInfo) {
	// not in db before, so we can just use make a entire new event
	if !oldEvent.inDB {
		oldEvent.mergedEvent = newEvent.dbChange
		oldEvent.originals = append(oldEvent.originals, newEvent.message)
		return
	}

	// need delete first then insert so we use replace
	oldEvent.mergedEvent.Operation = core.DBReplace
	oldEvent.mergedEvent.NewRow = newEvent.dbChange.NewRow
	oldEvent.originals = append(oldEvent.originals, newEvent.message)
}

// mergeUpdate for insert-update or update-update sequence
func mergeUpdate(oldEvent *core.DBChangeEvent, newEvent *core.DBChangeEvent) {
	for colName, value := range newEvent.NewRow {
		oldEvent.NewRow[colName] = value
	}
}

// splitByOperation split messages to insert, updates, deletes and replaces batches
func (bm *BatchMessage) splitByOperation() (batches [][]*MergedMessage) {
	inserts, updates, deletes, replaces := make([]*MergedMessage, 0), make([]*MergedMessage, 0), make([]*MergedMessage, 0), make([]*MergedMessage, 0)
	for _, each := range bm.mergedMessages {
		switch each.mergedEvent.Operation {
		case core.DBInsert:
			inserts = append(inserts, each)
		case core.DBUpdate:
			updates = append(updates, each)
		case core.DBDelete:
			deletes = append(deletes, each)
		case core.DBReplace:
			replaces = append(replaces, each)
		}
	}
	batches = [][]*MergedMessage{inserts, updates, deletes, replaces}
	return
}
