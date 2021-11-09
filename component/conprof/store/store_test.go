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
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const DBPath = "/Users/cs/code/ng-monitoring/data/docdb"

func TestReadData(t *testing.T) {
	return
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
	allSize := 0
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
		allSize += totalSize
		fmt.Printf("cnt: %v, index cnt: %v,size: %v MB, %v %v %v\n",
			totalCnt, len(list.TsList), totalSize/1024/1024,
			pt.Component, pt.Kind, pt.Address)
	}
	fmt.Printf("\nall size: %v MB", allSize/1024/1024)
	require.Equal(t, 1, 0)
}

func TestReadBadgerData(t *testing.T) {
	dataPath := DBPath
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

	db, err := badger.Open(opts)
	require.NoError(t, err)
	defer db.Close()

	start := time.Now()
	totalKeys := 0
	totalKeySize := 0
	totalValueSize := 0
	fmt.Printf("\n\n")
	db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			totalKeys++
			totalKeySize += len(k)
			err := item.Value(func(v []byte) error {
				totalValueSize += len(v)
				return nil
			})
			if err != nil {
				return err
			}
			if totalKeys%10 == 0 && time.Since(start) > time.Second*5 {
				start = time.Now()
				fmt.Printf("cnt: %v, key: %vMB, value: %vMB \n",
					totalKeys, totalKeySize/1024/1024, totalValueSize/1024/1024)
			}
		}
		return nil
	})
	fmt.Printf("cnt: %v, key: %vKB, value: %vMB \n",
		totalKeys, totalKeySize/1024, totalValueSize/1024/1024)

	require.Equal(t, 1, 0)
}

func newDB() *genji.DB {
	dataPath := DBPath
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
