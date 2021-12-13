package store

type Metric struct {
	Metric     topSQLTags `json:"metric"`
	Timestamps []uint64   `json:"timestamps"` // in millisecond
	Values     []uint32   `json:"values"`
}

type topSQLTags struct {
	SQLDigest    string `json:"sql_digest"`
	Name         string `json:"__name__"`
	PlanDigest   string `json:"plan_digest,omitempty"`
	Instance     string `json:"instance"`
	InstanceType string `json:"instance_type"`
}
