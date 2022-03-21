package document

import (
	"runtime"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	lastFlattenTsKey = []byte("last_flatten_ts")
	flattenInterval  = time.Minute * 1
)

func doGCLoop(db *badger.DB, closed chan struct{}) {
	log.Info("badger start to run value log gc loop")
	ticker := time.NewTicker(1 * time.Minute)
	defer func() {
		ticker.Stop()
		log.Info("badger stop running value log gc loop")
	}()

	// run gc when started.
	runGC(db)
	for {
		select {
		case <-ticker.C:
			runGC(db)
		case <-closed:
			return
		}
	}
}

func runGC(db *badger.DB) {
	defer func() {
		r := recover()
		if r != nil {
			log.Error("panic when run badger gc",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	tryFlattenIfNeeded(db)
	runValueLogGC(db)
}

func runValueLogGC(db *badger.DB) {
	// at most do 10 value log gc each time.
	for i := 0; i < 10; i++ {
		err := db.RunValueLogGC(0.001)
		if err != nil {
			if err == badger.ErrNoRewrite {
				log.Info("badger has no value log need gc now")
			} else {
				log.Error("badger run value log gc failed", zap.Error(err))
			}
			return
		}
		log.Info("badger run value log gc success")
	}
}

// tryFlattenIfNeeded try to do flatten if needed.
// Flatten uses to remove the old version keys, otherwise, the value log gc won't release the disk space which occupied
// by the old version keys.
func tryFlattenIfNeeded(db *badger.DB) {
	if !needFlatten(db) {
		return
	}
	start := time.Now()
	err := db.Flatten(runtime.NumCPU()/2 + 1)
	if err != nil {
		log.Error("badger flatten failed", zap.Error(err))
		return
	}
	ts := time.Now().Unix()
	err = storeLastFlattenTs(db, ts)
	if err != nil {
		log.Error("badger store last flatten ts failed", zap.Error(err))
		return
	}
	log.Info("badger flatten success", zap.Int64("ts", ts), zap.Duration("cost", time.Since(start)))
	ListBadgerDB(badgerDB, false)
	ListBadgerDB(badgerDB, true)
}

func needFlatten(db *badger.DB) bool {
	ts, err := getLastFlattenTs(db)
	if err != nil {
		log.Error("badger get last flatten ts failed", zap.Error(err))
	}
	interval := time.Now().Unix() - ts
	return time.Duration(interval)*time.Second >= flattenInterval
}

func getLastFlattenTs(db *badger.DB) (int64, error) {
	ts := int64(0)
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(lastFlattenTsKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		err = item.Value(func(val []byte) error {
			ts, err = strconv.ParseInt(string(val), 10, 64)
			return err
		})
		return err
	})
	return ts, err
}

func storeLastFlattenTs(db *badger.DB, ts int64) error {
	return db.Update(func(txn *badger.Txn) error {
		v := strconv.FormatInt(ts, 10)
		return txn.Set(lastFlattenTsKey, []byte(v))
	})
}

func ListBadgerDB(db *badger.DB, allVersion bool) error {
	keySize := 0
	valueSize := 0
	keyCount := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = allVersion
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				valueSize += len(v)
				return nil
			})
			if err != nil {
				return err
			}
			keySize += len(k)
			keyCount++
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Info("finish get total badger db size",
		zap.Float64("size(GB)", float64(keySize+valueSize)/GB),
		zap.Bool("all-version", allVersion),
		zap.Int("count", keyCount),
		zap.Float64("key-size(MB)", float64(keySize)/MB),
		zap.Float64("value-size(GB)", float64(valueSize)/GB),
	)
	return nil
}

const MB = 1024 * 1024
const GB = 1024 * 1024 * 1024
