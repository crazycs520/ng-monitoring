package store_test

import (
	"fmt"
	"github.com/pingcap/ng-monitoring/component/conprof/store"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ng-monitoring/component/conprof/meta"
	"github.com/pingcap/ng-monitoring/utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestProfileStorage(t *testing.T) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "ngm-test-.*")
	require.NoError(t, err)
	defer func() {
		size, err := DirSize(tmpDir)
		require.NoError(t, err)
		fmt.Printf("after close, db path size: %v MB\n", size/1024/1024)
		err = os.RemoveAll(tmpDir)
		require.NoError(t, err)
	}()
	genjiDB := testutil.NewGenjiDB(t, tmpDir)
	defer genjiDB.Close()
	storage, err := store.NewProfileStorage(genjiDB)
	require.NoError(t, err)
	defer storage.Close()

	baseTs := time.Now().Unix()
	testProfileStorage(t, storage, baseTs)

	size, err := DirSize(tmpDir)
	require.NoError(t, err)
	fmt.Printf("db path size: %v MB\n", size/1024/1024)
}

func testProfileStorage(t *testing.T, storage *store.ProfileStorage, baseTs int64) {
	profileDatas, err := readProfileData()
	require.NoError(t, err)

	concurrency := 40
	batchSize := 100

	lastPrint := time.Now()
	min := time.Hour
	max := time.Millisecond
	sum := time.Duration(0)
	cnt := 0
	for i := 0; i < batchSize; i++ {
		start := time.Now()
		testConcurrencyWriteDB(t, concurrency, storage, baseTs+int64(i), profileDatas)
		cost := time.Since(start)
		sum += cost
		cnt++
		if cost > max {
			max = cost
		}
		if cost < min {
			min = cost
		}

		if time.Since(lastPrint) >= time.Second*10 {
			lastPrint = time.Now()
			fmt.Printf("avg: %v, min: %v, max: %v , concurrency: %v\n", sum/time.Duration(cnt), min, max, concurrency)
			min = time.Hour
			max = time.Millisecond
			sum = time.Duration(0)
			cnt = 0
		}
	}
	fmt.Printf("avg: %v, min: %v, max: %v , concurrency: %v\n", sum/time.Duration(cnt), min, max, concurrency)

	param := &meta.BasicQueryParam{
		Begin: baseTs,
		End:   baseTs + int64(batchSize),
		Limit: 100,
	}

	start := time.Now()
	cnt = 0
	totalData := 0
	err = storage.QueryProfileData(param, func(target meta.ProfileTarget, timestamp int64, data []byte) error {
		cnt++
		totalData += len(data)
		return nil
	})
	require.Equal(t, concurrency*len(profileDatas)*batchSize, cnt)
	fmt.Printf("read data cnt: %v , total data size: %v MB, query cost: %v\n", cnt, totalData/1024/1024, time.Since(start))
	require.NoError(t, err)
}

func testConcurrencyWriteDB(t *testing.T, concurrency int, storage *store.ProfileStorage, baseTs int64, profileDatas []profileData) {
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		pt := meta.ProfileTarget{
			Component: "tidb",
			Address:   fmt.Sprintf("10.0.1.%v", 10+i),
		}
		ts := time.Unix(baseTs, 0)

		wg.Add(1)
		go func(pt meta.ProfileTarget, ts time.Time) {
			defer wg.Done()

			var wg0 sync.WaitGroup
			for _, data := range profileDatas {
				pt.Kind = data.name
				wg0.Add(1)
				go func(pt meta.ProfileTarget, ts time.Time, buf []byte) {
					defer wg0.Done()
					err := storage.AddProfile(pt, ts, buf)
					require.NoError(t, err)
				}(pt, ts, data.data)
			}
			wg0.Wait()
		}(pt, ts)
	}
	wg.Wait()
}

type profileData struct {
	name string
	data []byte
}

func readProfileData() ([]profileData, error) {
	profileDatas := make([]profileData, 0, 4)
	fileNames := []string{"goroutine", "heap", "mutex", "profile"}
	for _, name := range fileNames {
		data, err := ioutil.ReadFile(name)
		if err != nil {
			return nil, err
		}
		profileDatas = append(profileDatas, profileData{name: name, data: data})
	}
	return profileDatas, nil
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
