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

func (t *IcebergTable) CollectSnapshotMetrics(table types.TableIdentifier) error {
	query := fmt.Sprintf("select * from \"%s\".\"%s\".\"%s$history\"", table.Catalog, table.Schema, table.TableName)
	rows, err := t.DB.QueryContext(t.Ctx, query)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row types.SnapShotRow
		if err := rows.Scan(&row.MadeCurrentAt, &row.SnapShotID, &row.ParentID, &row.IsCurrentAncestor); err != nil {
			log.Fatalf("Error scanning row: %v", err)
		}
		fmt.Printf("SnapShotID: %d, ParentID: %d, IsCurrentAncestor: %v, MadeCurrentAt: %s\n",
			row.SnapShotID, row.ParentID, row.IsCurrentAncestor, row.MadeCurrentAt)
	}
	return nil
}

// getTables queries the information_schema for all BASE TABLES in the given catalog
func (t *IcebergTable) getTables(catalog string) ([]types.TableIdentifier, error) {
	query := `SELECT table_schema, table_name FROM ` + fmt.Sprintf("%s.information_schema.tables", catalog) + ` WHERE table_type = 'BASE TABLE'`
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
		tables = append(tables, types.TableIdentifier{Catalog: catalog, Schema: schemaName, TableName: tableName})
	}
	return tables, nil
}

// collectFileMetrics collects file count, average, median, and total size for a table
func (t *IcebergTable) collectFileMetrics(table types.TableIdentifier) (int64, float64, float64, float64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) AS file_count, AVG(file_size_in_bytes) / (1024 * 1024) AS avg_mb, APPROX_PERCENTILE(file_size_in_bytes, 0.5) / (1024 * 1024) AS median_mb, SUM(file_size_in_bytes) / (1024 * 1024 * 1024) AS total_gb FROM "%s"."%s"."%s$files"`, table.Catalog, table.Schema, table.TableName)
	row := t.DB.QueryRowContext(t.Ctx, query)
	var fileCount int64
	var avgMB, medianMB, totalGB float64
	if err := row.Scan(&fileCount, &avgMB, &medianMB, &totalGB); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("scan error: %w", err)
	}
	return fileCount, avgMB, medianMB, totalGB, nil
}

// collectPartitionMetrics collects partition and file count grouped by partition
func (t *IcebergTable) collectPartitionMetrics(table types.TableIdentifier) (map[string]int64, error) {
	query := fmt.Sprintf(`SELECT partition.created_at_day AS partition, COUNT(*) AS files FROM "%s"."%s"."%s$files" GROUP BY 1`, table.Catalog, table.Schema, table.TableName)
	rows, err := t.DB.QueryContext(t.Ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var partition string
		var files int64
		if err := rows.Scan(&partition, &files); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		result[partition] = files
	}
	return result, nil
}

// collectSnapshotCount collects the count of snapshots for a table
func (t *IcebergTable) collectSnapshotCount(table types.TableIdentifier) (int64, error) {
	query := fmt.Sprintf(`SELECT count(*) FROM "%s"."%s"."%s$snapshots"`, table.Catalog, table.Schema, table.TableName)
	row := t.DB.QueryRowContext(t.Ctx, query)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("scan error: %w", err)
	}
	return count, nil
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
