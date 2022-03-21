package mockload

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/component/conprof/store"
	"github.com/pingcap/ng-monitoring/config"
)

var (
	profileTargets = []string{"tidb", "tikv", "pd", "tiflash"}
	dataSizes      = []int{128 * 1024, 256 * 1024, 512 * 1024, 128 * 1024}
	datas          = [][]byte{}
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

func StartLoadData(db *store.ProfileStorage) {
	cfg := config.GetGlobalConfig()
	cfg.ContinueProfiling.DataRetentionSeconds = 60 * 5
	config.StoreGlobalConfig(cfg)

	total := 0
	for _, data := range datas {
		total += len(data)
	}
	log.Info("init finish", zap.Int("total_data_list_size(MB)", total/1024/1024), zap.Int("total_per_minute", total*len(profileTargets)*60/1024/1024))
	go startLoadData(db)
}

func startLoadData(db *store.ProfileStorage) {
	totalWritedSize := 0
	lastTime := time.Time{}
	lastLogTime := time.Now()
	for {
		start := time.Now()
		ts := start
		if ts.Unix() == lastTime.Unix() {
			ts = lastTime.Add(time.Second)
		}
		lastTime = ts
		for _, pt := range profileTargets {
			for i, data := range datas {
				data := genData(data)
				err := db.AddProfile(meta.ProfileTarget{
					Kind:      "heap" + strconv.Itoa(i),
					Component: pt,
					Address:   "0.0.0.0:4000",
				}, ts, data, nil)
				if err != nil {
					log.Warn("write data error", zap.Error(err))
				}
				totalWritedSize += len(data)
			}
		}
		cost := time.Since(start)
		sleep := time.Second - cost
		if sleep > 0 {
			time.Sleep(sleep)
		}

		if time.Since(lastLogTime) > time.Second*5 {
			lastLogTime = time.Now()
			dirSize, _ := DirSize(config.GetGlobalConfig().Storage.Path)
			log.Info("report load data", zap.Int("writed_data(MB)", totalWritedSize/1024/1024), zap.Int64("dir_size(MB)", dirSize/1024/1024))
		}
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

func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
