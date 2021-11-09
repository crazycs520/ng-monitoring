package store

import (
	"context"
	"fmt"
	"github.com/pingcap/ng_monitoring/component/conprof/meta"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/database/document"
	"github.com/stretchr/testify/require"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func TestReadData(t *testing.T) {
	db := newDB()
	defer db.Close()

	storage, err := NewProfileStorage(db)
	require.NoError(t, err)
	fmt.Printf("\n\n\ntotal meta size: %v\n", len(storage.metaCache))
	for k, v := range storage.metaCache {
		fmt.Println(k.Component, k.Kind, k.Address, v.ID, v.LastScrapeTs)
	}
	param := &meta.BasicQueryParam{
		Begin: 0,
		End:   math.MaxInt64,
		Limit: 100000000000,
	}
	for pt, info := range storage.metaCache {
		totalSize := 0
		totalCnt := 0
		err := storage.QueryTargetProfileData(pt, info, param, func(target meta.ProfileTarget, i int64, bytes []byte) error {
			totalCnt++
			totalSize += len(bytes)
			return nil
		})
		require.NoError(t, err)

		list, err := storage.QueryTargetProfiles(pt, info, param)
		require.NoError(t, err)
		fmt.Printf("cnt: %v, index cnt: %v,size: %v MB, %v %v %v\n",
			totalCnt, len(list.TsList), totalSize/1024/1024,
			pt.Component, pt.Kind, pt.Address)
	}

	require.Equal(t, 1, 0)
}

func newDB() *genji.DB {
	dataPath := "/Users/cs/code/ng-monitoring/data/docdb"
	l, err := document.SimpleLogger(&config.Log{
		Path:  "",
		Level: config.LevelInfo,
	})
	if err != nil {
		log.Fatal("failed to open a badger log", zap.String("path", dataPath), zap.Error(err))
	}
	opts := badger.DefaultOptions(dataPath).
		WithCompression(options.ZSTD).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).WithLogger(l)

	engine, err := badgerengine.NewEngine(opts)
	if err != nil {
		log.Fatal("failed to open a badger storage", zap.String("path", dataPath), zap.Error(err))
	}

	db, err := genji.New(context.Background(), engine)
	if err != nil {
		log.Fatal("failed to open a document database", zap.String("path", dataPath), zap.Error(err))
	}
	return db
}
