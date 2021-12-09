package store

import (
	"bytes"
	"github.com/pingcap/log"
	"github.com/pingcap/ng_monitoring/config"
	"github.com/pingcap/ng_monitoring/database"
	"github.com/pingcap/ng_monitoring/database/document"
	"github.com/pingcap/ng_monitoring/database/timeseries"
	"github.com/pingcap/ng_monitoring/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"net/http"
	"testing"
	"time"
)

func TestStoreBasic(t *testing.T) {
	defCfg := config.GetDefaultConfig()
	config.StoreGlobalConfig(&defCfg)
	cfg := config.GetGlobalConfig()

	database.Init(cfg)
	defer database.Stop()

	db := document.Get()
	Init(timeseries.InsertHandler, db)

	ts := uint64(time.Now().UnixNano() / int64(time.Millisecond))
	metric := Metric{
		Metric: topSQLTags{
			Name:         "profile",
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
	require.Equal(t, 200, respR.Code)
}
