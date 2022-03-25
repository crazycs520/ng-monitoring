package document

import (
	"context"
	"path"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/utils"
	"go.uber.org/zap"
)

var documentDB *genji.DB
var badgerDB *badger.DB
var closeCh chan struct{}

func Init(cfg *config.Config) {
	dataPath := path.Join(cfg.Storage.Path, "docdb")
	l, _ := InitLogger(cfg)
	opts := badger.DefaultOptions(dataPath).
		WithNumLevelZeroTables(2).
		WithNumVersionsToKeep(0).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).
		WithLogger(l)

	engine, err := badgerengine.NewEngine(opts)
	if err != nil {
		log.Fatal("failed to open a badger storage", zap.String("path", dataPath), zap.Error(err))
	}

	closeCh = make(chan struct{})
	go utils.GoWithRecovery(func() {
		doGCLoop(engine.DB, closeCh)
	}, nil)

	db, err := genji.New(context.Background(), engine)
	if err != nil {
		log.Fatal("failed to open a document database", zap.String("path", dataPath), zap.Error(err))
	}
	documentDB = db
	badgerDB = engine.DB
}

func Get() *genji.DB {
	return documentDB
}

func GetBadger() *badger.DB {
	return badgerDB
}

func Stop() {
	close(closeCh)
	if err := documentDB.Close(); err != nil {
		log.Fatal("failed to close the document database", zap.Error(err))
	}
}
