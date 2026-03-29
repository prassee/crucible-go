// Package client provides Trino database client functionality for collecting and storing Iceberg table metrics.
package client

import (
	"context"
	"crucible-go/src/types"
	"database/sql"
	"fmt"
	"log"
	"time"
)

type IcebergTable struct {
	Ctx context.Context
	DB  *sql.DB
}

func (t *IcebergTable) GetTables(catalog string) ([]types.TableIdentifier, error) {
	query := `SELECT table_schema, table_name FROM ` + fmt.Sprintf("%s.information_schema.tables", catalog) + ` WHERE table_type = 'BASE TABLE'`
	log.Printf("Executing get tables query: %s", query)
	rows, err := t.DB.QueryContext(t.Ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()
	var tables []types.TableIdentifier
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		if schemaName == "information_schema" || schemaName == "system" {
			continue
		}
		// for i := range 1000 {
		tables = append(tables, types.TableIdentifier{Catalog: catalog, Schema: schemaName, TableName: tableName})
		// }
	}
	return tables, nil
}

// CollectMetrics collects file and snapshot metrics for a table and returns a TableMetric instance
func (t *IcebergTable) CollectMetrics(table types.TableIdentifier) (*types.TableMetric, error) {
	fqn := table.Catalog + "." + table.Schema + "." + table.TableName
	// File metrics
	fileQuery := fmt.Sprintf(`SELECT COUNT(*) AS file_count, AVG(file_size_in_bytes) / (1024 * 1024) AS avg_mb, SUM(file_size_in_bytes) / (1024 * 1024 * 1024) AS total_gb FROM "%s"."%s"."%s$files"`, table.Catalog, table.Schema, table.TableName)
	row := t.DB.QueryRowContext(t.Ctx, fileQuery)
	var fileCount int64
	var avgMBNull, totalGBNull sql.NullFloat64
	if err := row.Scan(&fileCount, &avgMBNull, &totalGBNull); err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}
	var avgMB, totalGB float64
	if avgMBNull.Valid {
		avgMB = avgMBNull.Float64
	}
	if totalGBNull.Valid {
		totalGB = totalGBNull.Float64
	}

	// Snapshot count
	snapQuery := fmt.Sprintf(`SELECT count(*) FROM "%s"."%s"."%s$snapshots"`, table.Catalog, table.Schema, table.TableName)
	// log.Printf("Executing file metrics query for %s.%s.%s - \n %s", table.Catalog, table.Schema, table.TableName, fileQuery)
	// log.Printf("Executing snapshot metrics query for %s.%s.%s - \n %s", table.Catalog, table.Schema, table.TableName, snapQuery)
	snapRow := t.DB.QueryRowContext(t.Ctx, snapQuery)
	var snapshotCount int64
	if err := snapRow.Scan(&snapshotCount); err != nil {
		return nil, fmt.Errorf("scan error: %w", err)
	}

	metric := &types.TableMetric{
		FQN:           fqn,
		FileCount:     sql.NullInt64{Int64: fileCount, Valid: true},
		AvgMB:         sql.NullFloat64{Float64: avgMB, Valid: true},
		MedianMB:      sql.NullFloat64{Float64: avgMB, Valid: true}, // Assuming median is same as avg for now
		TotalGB:       sql.NullFloat64{Float64: totalGB, Valid: true},
		SnapshotCount: sql.NullInt64{Int64: snapshotCount, Valid: true},
		CollectedAt:   time.Now(),
	}
	return metric, nil
}
