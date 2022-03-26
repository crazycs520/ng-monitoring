package mockload

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ng-monitoring/database/document"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/component/conprof/store"
	"github.com/pingcap/ng-monitoring/config"
)

var (
	profileTargets = []string{"tidb", "tikv", "pd", "tiflash", "tidb2"}
	dataSizes      = []int{120 * 1024, 256 * 1024, 512 * 1024, 128 * 1024, 50 * 1024}
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

func StartLoadData(db *store.ProfileStorage, mockLoad bool) {
	cfg := config.GetGlobalConfig()
	cfg.ContinueProfiling.DataRetentionSeconds = 60 * 1
	config.StoreGlobalConfig(cfg)

	badgerDB := document.GetBadger()
	ListBadgerDB(badgerDB, false)
	ListBadgerDB(badgerDB, true)
	infos := strings.Split(badgerDB.LevelsToString(), "\n")
	for _, info := range infos {
		log.Info(info)
	}

	total := 0
	for _, data := range datas {
		total += len(data)
	}
	log.Info("init finish", zap.Int("total_data_list_size(MB)", total/1024/1024), zap.Int("total_per_minute", total*len(profileTargets)*60/1024/1024))
	if mockLoad {
		go startLoadData(db)
	}
	go func() {
		for {
			time.Sleep(time.Minute)
			badgerDB := document.GetBadger()
			ListBadgerDB(badgerDB, false)
			ListBadgerDB(badgerDB, true)
			infos := strings.Split(badgerDB.LevelsToString(), "\n")
			for _, info := range infos {
				log.Info(info)
			}
		}
	}()
}

func startLoadData(db *store.ProfileStorage) {
	totalWritedSize := 0
	lastTime := time.Time{}
	lastLogTime := time.Now()
	ticker := time.NewTicker(time.Minute)
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

		select {
		case <-ticker.C:
			for _, pt := range profileTargets {
				for i, _ := range datas {
					_, err := db.UpdateProfileTargetInfo(meta.ProfileTarget{
						Kind:      "heap" + strconv.Itoa(i),
						Component: pt,
						Address:   "0.0.0.0:4000",
					}, ts.Unix())
					if err != nil {
						log.Warn("write data error", zap.Error(err))
					}
				}
			}
		default:
		}

		if time.Since(lastLogTime) > time.Second*10 {
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
