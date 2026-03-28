package client

import (
	"context"
	"crucible-go/src/types"
	"database/sql"
	"fmt"
	"time"
)

type IcebergTable struct {
	Ctx context.Context
	DB  *sql.DB
}

// CollectMetrics collects file and snapshot metrics for a table and returns a TableMetric instance
func (t *IcebergTable) CollectMetrics(table types.TableIdentifier) (*types.TableMetric, error) {
	fqn := table.Catalog + "." + table.Schema + "." + table.TableName

	// File metrics
	fileQuery := `SELECT COUNT(*) AS file_count, AVG(file_size_in_bytes) / (1024 * 1024) AS avg_mb, SUM(file_size_in_bytes) / (1024 * 1024 * 1024) AS total_gb FROM "` + table.Catalog + `"."` + table.Schema + `"."` + table.TableName + `$files"`
	row := t.DB.QueryRowContext(t.Ctx, fileQuery)
	var fileCount int64
	var avgMB, totalGB float64
	if err := row.Scan(&fileCount, &avgMB, &totalGB); err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	// Snapshot count
	snapQuery := `SELECT count(*) FROM "` + table.Catalog + `"."` + table.Schema + `"."` + table.TableName + `$snapshots"`
	snapRow := t.DB.QueryRowContext(t.Ctx, snapQuery)
	var snapshotCount int64
	if err := snapRow.Scan(&snapshotCount); err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	metric := &types.TableMetric{
		FQN:           fqn,
		FileCount:     fileCount,
		AvgMB:         avgMB,
		TotalGB:       totalGB,
		SnapshotCount: snapshotCount,
		CollectedAt:   time.Now(),
	}
	return metric, nil
}
