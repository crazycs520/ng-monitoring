package debug

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/component/conprof/store"
	"go.uber.org/zap"
)

func ListDBData(genji *genji.DB, badger *badger.DB) error {
	err := ListDocDBData(genji)
	if err != nil {
		return nil
	}
	return ListBadgerDB(badger)
}

func ListBadgerDB(db *badger.DB) error {
	keySize := 0
	valueSize := 0
	keyCount := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
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
		zap.Int("count", keyCount),
		zap.Float64("key-size(MB)", float64(keySize)/MB),
		zap.Float64("value-size(GB)", float64(valueSize)/GB),
	)
	return nil
}

func ListDocDBData(db *genji.DB) error {
	tables, err := ListDocDBTables(db)
	if err != nil {
		return err
	}
	sort.Strings(tables)
	tm, err := loadAllTargetsFromTable(db)
	if err != nil {
		return err
	}
	totalSize := 0
	totalCount := 0
	for _, table := range tables {
		size, count, err := getTableDataSize(db, table)
		if err != nil {
			return err
		}
		pt := getProfileTarget(tm, table)
		if pt == nil {
			log.Info("finish get table size", zap.String("table", table), zap.Int("rows", count), zap.Float64("size(GB)", float64(size)/GB))
		} else {
			t := time.Unix(pt.LastScrapeTs, 0)
			log.Info("finish get table size", zap.String("table", table), zap.Int("rows", count), zap.Float64("size(GB)", float64(size)/GB),
				zap.String("component", pt.Component),
				zap.String("kind", pt.Kind),
				zap.String("addr", pt.Address),
				zap.String("last_scrape", t.String()),
			)
		}
		totalSize += size
		totalCount += count
	}
	log.Info("finish get total table size", zap.Float64("size(GB)", float64(totalSize)/GB), zap.Int("rows", totalCount))
	return nil
}

const MB = 1024 * 1024
const GB = 1024 * 1024 * 1024

func getTableDataSize(db *genji.DB, table string) (int, int, error) {
	query := fmt.Sprintf("SELECT * from %v", table)
	res, err := db.Query(query)
	if err != nil {
		return 0, 0, err
	}
	defer res.Close()

	totalSize := 0
	count := 0
	err = res.Iterate(func(d types.Document) error {
		count++
		return d.Iterate(func(_ string, value types.Value) error {
			size := getValueSize(value)
			totalSize += size
			return nil
		})
	})
	return totalSize, count, nil
}

func getValueSize(value types.Value) int {
	switch value.Type() {
	case types.NullValue:
		return 1
	case types.BoolValue:
		return 1
	case types.IntegerValue:
		return 8
	case types.DoubleValue:
		return 8
	default:
		v := value.V()
		switch tv := v.(type) {
		case string:
			return len(tv)
		case []byte:
			return len(tv)
		}
		str := fmt.Sprintf("%v", value.V())
		return len(str)
	}
}

func ListDocDBTables(db *genji.DB) ([]string, error) {
	tables := []string{}
	res, err := db.Query("SELECT name FROM __genji_catalog WHERE type = 'table';")
	if err != nil {
		return nil, err
	}
	defer res.Close()

	err = res.Iterate(func(d types.Document) error {
		var tb string
		err := document.Scan(d, &tb)
		if err != nil {
			return err
		}
		tables = append(tables, tb)
		return nil
	})
	return tables, nil
}

func getProfileTarget(tm map[int64]*ProfileTarget, table string) *ProfileTarget {
	fields := strings.Split(table, "_")
	if len(fields) != 3 || fields[0] != "conprof" || fields[2] != "data" {
		return nil
	}
	id, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return nil
	}
	return tm[id]
}

type ProfileTarget struct {
	ID           int64
	LastScrapeTs int64

	Kind      string `json:"kind"`
	Component string `json:"component"`
	Address   string `json:"address"`
}

func loadAllTargetsFromTable(db *genji.DB) (map[int64]*ProfileTarget, error) {
	query := fmt.Sprintf("SELECT id, kind, component, address, last_scrape_ts FROM %v", store.MetaTableName)
	res, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	tm := make(map[int64]*ProfileTarget)
	err = res.Iterate(func(d types.Document) error {
		var id, ts int64
		var kind, component, address string
		err = document.Scan(d, &id, &kind, &component, &address, &ts)
		if err != nil {
			return err
		}
		tm[id] = &ProfileTarget{
			ID:           id,
			LastScrapeTs: ts,
			Kind:         kind,
			Component:    component,
			Address:      address,
		}
		return nil
	})
	return tm, nil
}
