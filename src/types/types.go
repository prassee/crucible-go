package types

import (
	"time"
)

type TableIdentifier struct {
	Catalog   string
	Schema    string
	TableName string
}

type SnapShotRow struct {
	MadeCurrentAt     string
	SnapShotID        int64
	ParentID          int64
	IsCurrentAncestor bool
}

type IbTableAggregate struct {
	Catalog   string
	Schema    string
	TableName string
	SnapShots int32
}

type TableConfig struct {
	Catalog   string `yaml:"catalog"`
	Schema    string `yaml:"schema"`
	TableName string `yaml:"table_name"`
}

type TableMetric struct {
	FQN           string // e.g., "lakekeeper.db.orders"
	FileCount     int64
	AvgMB         float64
	TotalGB       float64
	SnapshotCount int64
	CollectedAt   time.Time
}
