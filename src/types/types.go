// Package types defines the core data structures used throughout the Crucible application, including configuration, table identifiers, and metrics.
package types

import (
	"database/sql"
	"time"
)

type TableIdentifier struct {
	Catalog   string
	Schema    string
	TableName string
}

type CrucibleConfig struct {
	TrinoDSN string `yaml:"trino_dsn"`
	Catalog  string `yaml:"catalog"`
	Workers  int    `yaml:"workers"`
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
	FileCount     sql.NullInt64
	AvgMB         sql.NullFloat64
	MedianMB      sql.NullFloat64
	TotalGB       sql.NullFloat64
	SnapshotCount sql.NullInt64
	CollectedAt   time.Time
}

var avgMBNull, totalGBNull sql.NullFloat64

func (t *TableMetric) GetAvgMB() float64 {
	if avgMBNull.Valid {
		return avgMBNull.Float64
	}
	return 0
}

func (t *TableMetric) GetTotalGB() float64 {
	if totalGBNull.Valid {
		return totalGBNull.Float64
	}
	return 0
}
