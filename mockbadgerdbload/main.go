package main

import (
	"context"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/genjidb/genji"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/config"
	"github.com/pingcap/ng-monitoring/database/document"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji/engine/badgerengine"
)

var (
	dataSizes = []int{120 * 1024, 256 * 1024, 512 * 1024, 128 * 1024, 50 * 1024}
	datas     = [][]byte{}
)

func init() {
	for _, size := range dataSizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte((rand.Int() % 256))
		}
		datas = append(datas, data)
	}
}
func main() {
	//db, err := NewBadgerDB("data")
	//mustBeNil(err)
	//defer db.Close()
	genjidb, db, err := NewGenjiDB("data")
	mustBeNil(err)
	defer genjidb.Close()

	//genjiPrepare(genjidb)

	//txn := db.NewTransaction(true)
	//err = txn.Commit()
	//mustBeNil(err)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			if isCtxDone(ctx) {
				return
			}
			badgerDelete(db)
			//genjiDelete(genjidb)
			time.Sleep(time.Second * 1)
		}
	}()

	go func() {
		lastF := time.Now()
		for {
			if time.Since(lastF) > time.Second*30 {
				lastF = time.Now()
				db.Flatten(12)
				fmt.Printf("flatten----------\n")
			}
			runValueLogGC(db)
			fmt.Printf("max version: %v\n", db.MaxVersion())
			//db.SetDiscardTs()
			select {
			case <-time.After(time.Second * 10):
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for {
			ListBadgerDB(db, false)
			ListBadgerDB(db, true)
			infos := strings.Split(db.LevelsToString(), "\n")
			for _, info := range infos {
				log.Info(info)
			}
			select {
			case <-time.After(time.Second * 10):
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for {
			if isCtxDone(ctx) {
				return
			}
			badgerWrite(db)
			//genjiWrite(genjidb)
			time.Sleep(time.Millisecond * 100)
		}
	}()

	sig := procutil.WaitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
	cancel()
	time.Sleep(time.Second * 2)
}

func runValueLogGC(db *badger.DB) {
	// at most do 10 value log gc each time.
	for i := 0; i < 10; i++ {
		err := db.RunValueLogGC(0.1)
		if err != nil {
			if err == badger.ErrNoRewrite {
				fmt.Println("badger has no value log need gc now")
			} else {
				fmt.Println("badger run value log gc failed", err)
			}
			return
		}
		fmt.Println("badger run value log gc success------------")
	}
}

func badgerWrite(db *badger.DB) {
	db.Update(func(txn *badger.Txn) error {
		ts := time.Now().UnixNano()
		for i, data := range datas {
			key := strconv.Itoa(int(ts) + i)
			txn.Set([]byte(key), genData(data))
		}
		return nil
	})
}

func badgerDelete(db *badger.DB) {
	deletedTsList := [][]byte{}
	oldestTS := time.Now().Add(-time.Second * 10).UnixNano()
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			ts, err := strconv.ParseInt(string(k), 10, 64)
			if err != nil {
				continue
			}
			if ts < oldestTS {
				deletedTsList = append(deletedTsList, k)
			} else {
				break
			}
		}
		return nil
	})
	mustBeNil(err)

	err = db.Update(func(txn *badger.Txn) error {
		for _, k := range deletedTsList {
			err := txn.Delete(k)
			mustBeNil(err)
		}
		return nil
	})
	mustBeNil(err)
	fmt.Printf("delete %v items\n", len(deletedTsList))
}

func genjiWrite(db *genji.DB) {
	ts := time.Now().UnixNano()
	for i, data := range datas {
		err := db.Exec("INSERT INTO t (ts,data) VALUES (?, ?)", int(ts)+i, data)
		mustBeNil(err)
	}
}

func genjiDelete(db *genji.DB) {
	oldestTS := time.Now().Add(-time.Second * 10).UnixNano()
	err := db.Exec("DELETE FROM t WHERE ts <= ?", oldestTS)
	mustBeNil(err)
}

func genjiPrepare(db *genji.DB) {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS t (ts INTEGER PRIMARY KEY, data BLOB)")
	err := db.Exec(sql)
	mustBeNil(err)
}

func NewBadgerDB(storagePath string) (*badger.DB, error) {
	cfg := config.GetDefaultConfig()
	cfg.Log.Path = "log"
	l, _ := document.InitLogger(&cfg)
	opts := badger.DefaultOptions(storagePath).
		WithNumLevelZeroTables(2).
		WithNumVersionsToKeep(0).
		WithZSTDCompressionLevel(3).
		//WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).
		WithLogger(l)

	engine, err := badgerengine.NewEngine(opts)
	return engine.DB, err
}

func NewGenjiDB(storagePath string) (*genji.DB, *badger.DB, error) {
	cfg := config.GetDefaultConfig()
	cfg.Log.Path = "log"
	l, _ := document.InitLogger(&cfg)
	opts := badger.DefaultOptions(storagePath).
		WithNumLevelZeroTables(2).
		WithNumVersionsToKeep(0).
		WithZSTDCompressionLevel(3).
		//WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024).
		WithLogger(l)

	engine, err := badgerengine.NewEngine(opts)
	if err != nil {
		return nil, nil, err
	}

	db, err := genji.New(context.Background(), engine)
	return db, engine.DB, err
}

func mustBeNil(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
}

func genData(src []byte) []byte {
	data := make([]byte, len(src))
	copy(data, src)
	for i := 0; i < 100; i++ {
		n := rand.Int()
		idx := n % len(data)
		data[idx] = byte(n % 256)
	}
	return data
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

func isCtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
