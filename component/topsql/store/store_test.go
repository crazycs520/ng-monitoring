package store

import (
	"bytes"
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/database"
	"github.com/pingcap/ng_monitoring/database/document"
	"github.com/pingcap/ng_monitoring/database/timeseries"
	"github.com/pingcap/ng_monitoring/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"
)

func TestStoreBasic(t *testing.T) {
	defCfg := config.GetDefaultConfig()
	defCfg.Storage.Path = "/tmp/data"
	config.StoreGlobalConfig(&defCfg)
	cfg := config.GetGlobalConfig()
	defer func() {
		//os.RemoveAll(cfg.Storage.Path)
	}()

	database.Init(cfg)
	defer database.Stop()

	db := document.Get()
	Init(timeseries.InsertHandler, db)

	ts := uint64(1633435143)
	insertFn := func(metric Metric) {
		buf := bytes.NewBuffer(nil)
		err := encodeMetric(buf, metric)
		require.NoError(t, err)

		header := http.Header{}
		respR := utils.NewRespWriter(buf, header)
		//log.Info("------", zap.String("body", buf.String()))
		req, err := http.NewRequest("POST", "/api/v1/import", buf)
		require.NoError(t, err)
		timeseries.InsertHandler(&respR, req)
		require.True(t, respR.Code == 200 || respR.Code == 204)
	}
	batchCnt := 1024
	batch := 2048
	for i := 0; i < batchCnt; i++ {
		metric := Metric{
			Metric: topSQLTags{
				Name:     "cpu",
				Instance: "tidb-0",
			},
		}
		for j := 0; j < batch; j++ {
			metric.Timestamps = append(metric.Timestamps, (ts+uint64(i*batchCnt+j)*10)*1000)
			metric.Values = append(metric.Values, uint32(100+j))
		}
		insertFn(metric)
		//_ = insertFn
	}

	// test for query
	query := fmt.Sprintf("cpu")
	start := strconv.Itoa(int(ts))
	end := strconv.Itoa(int(ts + 10))

	req, err := http.NewRequest("GET", "/api/v1/query_range", nil)
	require.NoError(t, err)
	reqQuery := req.URL.Query()
	reqQuery.Set("query", query)
	reqQuery.Set("start", start)
	reqQuery.Set("end", end)
	reqQuery.Set("step", strconv.Itoa(1))
	req.URL.RawQuery = reqQuery.Encode()
	req.Header.Set("Accept", "application/json")

	buf := bytes.NewBuffer(nil)
	header := http.Header{}
	respR := utils.NewRespWriter(buf, header)
	timeseries.SelectHandler(&respR, req)
	data, err := ioutil.ReadAll(respR.Body)
	require.NoError(t, err)
	log.Info("-----query data----", zap.String("body", string(data)))

	//require.Equal(t, 200, respR.Code)
}
