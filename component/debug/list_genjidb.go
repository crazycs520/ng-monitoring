package debug

import (
	"fmt"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/document"
	"github.com/genjidb/genji/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func ListDocDBData(db *genji.DB) error {
	tables, err := ListDocDBTables(db)
	if err != nil {
		return err
	}
	totalSize := 0
	for _, table := range tables {
		size, err := getTableDataSize(db, table)
		if err != nil {
			return err
		}
		totalSize += size
	}
	log.Info("finish get total table size", zap.Float64("size(GB)", float64(totalSize)/GB))
	return nil
}

const GB = 1024 * 1024 * 1024

func getTableDataSize(db *genji.DB, table string) (int, error) {
	query := fmt.Sprintf("SELECT * from %v", table)
	res, err := db.Query(query)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	totalSize := 0
	err = res.Iterate(func(d types.Document) error {
		return d.Iterate(func(_ string, value types.Value) error {
			size := getValueSize(value)
			totalSize += size
			return nil
		})
	})
	log.Info("finish get table size", zap.String("table", table), zap.Float64("size(GB)", float64(totalSize)/GB))
	return totalSize, nil
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
	res, err := db.Query("SELECT name FROM __genji_catalog WHERE type = 'table' AND name != '__genji_sequence';")
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
