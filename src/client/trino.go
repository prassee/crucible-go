package client

import (
	"context"
	"crucible-go/src/types"
	"database/sql"
	"fmt"
	"log"
)

func CollectSnapshotMetrics(ctx context.Context, table types.TableIdentifier, db *sql.DB) error {
	query := fmt.Sprintf("select * from \"%s\".\"%s\".\"%s$history\"", table.Catalog, table.Schema, table.TableName)
	rows, err := db.QueryContext(ctx, query)
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

// GetIcebergTables queries the information_schema for all BASE TABLES in the given catalog
func GetIcebergTables(ctx context.Context, db *sql.DB, catalog string) ([]types.TableIdentifier, error) {
	query := `SELECT table_schema, table_name FROM ` + fmt.Sprintf("%s.information_schema.tables", catalog) + ` WHERE table_type = 'BASE TABLE'`
	rows, err := db.QueryContext(ctx, query)
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

// CollectFileMetrics collects file count, average, median, and total size for a table
func CollectFileMetrics(ctx context.Context, db *sql.DB, table types.TableIdentifier) (int64, float64, float64, float64, error) {
	query := fmt.Sprintf(`SELECT COUNT(*) AS file_count, AVG(file_size_in_bytes) / (1024 * 1024) AS avg_mb, APPROX_PERCENTILE(file_size_in_bytes, 0.5) / (1024 * 1024) AS median_mb, SUM(file_size_in_bytes) / (1024 * 1024 * 1024) AS total_gb FROM "%s"."%s"."%s$files"`, table.Catalog, table.Schema, table.TableName)
	row := db.QueryRowContext(ctx, query)
	var fileCount int64
	var avgMB, medianMB, totalGB float64
	if err := row.Scan(&fileCount, &avgMB, &medianMB, &totalGB); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("scan error: %w", err)
	}
	return fileCount, avgMB, medianMB, totalGB, nil
}

// CollectPartitionMetrics collects partition and file count grouped by partition
func CollectPartitionMetrics(ctx context.Context, db *sql.DB, table types.TableIdentifier) (map[string]int64, error) {
	query := fmt.Sprintf(`SELECT partition.created_at_day AS partition, COUNT(*) AS files FROM "%s"."%s"."%s$files" GROUP BY 1`, table.Catalog, table.Schema, table.TableName)
	rows, err := db.QueryContext(ctx, query)
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

// CollectSnapshotCount collects the count of snapshots for a table
func CollectSnapshotCount(ctx context.Context, db *sql.DB, table types.TableIdentifier) (int64, error) {
	query := fmt.Sprintf(`SELECT count(*) FROM "%s"."%s"."%s$snapshots"`, table.Catalog, table.Schema, table.TableName)
	row := db.QueryRowContext(ctx, query)
	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("scan error: %w", err)
	}
	return count, nil
}
