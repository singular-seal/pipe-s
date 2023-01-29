package statestore

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/core"
	"github.com/singular-seal/pipe-s/pkg/statestore/file"
	"github.com/singular-seal/pipe-s/pkg/statestore/zookeeper"
	"github.com/singular-seal/pipe-s/pkg/utils"
)

func CreateStateStore(config core.StringMap) (store core.StateStore, err error) {
	var st string
	st, err = utils.GetStringFromConfig(config, "$.Type")
	if err != nil {
		return
	}
	switch st {
	case core.FileStateStore:
		store = file.NewFileStateStore()
		err = store.Configure(config)
		return
	case core.ZooKeeperStateStore:
		store = zookeeper.NewZKStateStore()
		err = store.Configure(config)
		return
	default:
		err = fmt.Errorf("unknown statestore type:%s", st)
		return
	}
}
