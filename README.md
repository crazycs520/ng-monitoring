[![LICENSE](https://img.shields.io/github/license/pingcap/tidb.svg)](https://github.com/pingcap/ng-monitoring/blob/main/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)

## What is NG-Monitoring?

NG-Monitoring is the Next Generation Monitoring Server for [TiDB](https://github.com/pingcap/tidb). It provides the following features:

- TiDB cluster TopSQL data storage.

- Continuous Profiling for TiDB cluster.

NG-Monitoring is a backend service, should be used with [tidb-dashboard](https://github.com/pingcap/tidb-dashboard)

## Usage

build
```shell
make
```

run
```shell
bin/ng-monitoring-server
```

See more usage detail here: [usage_detail.md](usage_detail.md)