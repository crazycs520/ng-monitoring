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
	config.StoreGlobalConfig(&defCfg)
	cfg := config.GetGlobalConfig()
	defer func() {
		//os.RemoveAll(cfg.Storage.Path)
	}()

	database.Init(cfg)
	defer database.Stop()

	db := document.Get()
	Init(timeseries.InsertHandler, db)

	//ts := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	ts := uint64(1639239169025)
	metric := Metric{
		Metric: topSQLTags{
			Name:         "cpu_time",
			Instance:     "10.0.1.8",
			InstanceType: "tidb",
			SQLDigest:    "abcdefghijk",
			PlanDigest:   "12345678901",
		},
		Timestamps: []uint64{ts},
		Values:     []uint32{120},
	}

	buf := bytes.NewBuffer(nil)
	err := encodeMetric(buf, metric)
	require.NoError(t, err)

	header := http.Header{}
	respR := utils.NewRespWriter(buf, header)
	log.Info("------", zap.String("body", buf.String()))
	req, err := http.NewRequest("POST", "/api/v1/import", buf)
	require.NoError(t, err)
	timeseries.InsertHandler(&respR, req)

	// test for query
	//query := fmt.Sprintf("sum_over_time(cpu_time{instance=\"%s\"}[%d])", metric.Metric.Instance, 1)
	query := fmt.Sprintf("cpu_time{instance=\"%s\"}", metric.Metric.Instance)
	//start := strconv.Itoa(int(time.Now().Unix() - 60*60*5))
	//end := strconv.Itoa(int(time.Now().Unix() + 60))
	start := strconv.Itoa(int(ts / 1000))
	end := strconv.Itoa(int(ts/1000 + 1))

	req, err = http.NewRequest("GET", "/api/v1/query_range", nil)
	require.NoError(t, err)
	reqQuery := req.URL.Query()
	reqQuery.Set("query", query)
	reqQuery.Set("start", start)
	reqQuery.Set("end", end)
	reqQuery.Set("step", strconv.Itoa(1))
	req.URL.RawQuery = reqQuery.Encode()
	req.Header.Set("Accept", "application/json")

	buf = bytes.NewBuffer(nil)
	respR = utils.NewRespWriter(buf, header)
	timeseries.SelectHandler(&respR, req)
	data, err := ioutil.ReadAll(respR.Body)
	require.NoError(t, err)
	log.Info("-----query data----", zap.String("body", string(data)))

	//require.Equal(t, 200, respR.Code)
}
