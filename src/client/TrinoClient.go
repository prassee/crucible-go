package client

import (
	"context"
	"crucible-go/src/types"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type TrinoClient struct {
	DSN string
	DB  *sql.DB
}

func NewTrinoClient(dsn string) (*TrinoClient, error) {
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, err
	}
	return &TrinoClient{DSN: dsn, DB: db}, nil
}

func (c *TrinoClient) Close() error {
	return c.DB.Close()
}

func (c *TrinoClient) ScanTableMetrics(ctx context.Context, catalog string, workersCount int) []*types.TableMetric {
	icebergTable := &IcebergTable{Ctx: ctx, DB: c.DB}
	tables, err := icebergTable.GetTables(catalog)
	if err != nil {
		log.Fatalf("Error getting tables: %v", err)
	}
	jobs := make(chan types.TableIdentifier, len(tables))
	metrics := make([]*types.TableMetric, 0, len(tables))
	var wg sync.WaitGroup

	for i := 1; i <= workersCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for table := range jobs {
				log.Printf("Worker %d processing table: %s.%s.%s", workerID, table.Catalog, table.Schema, table.TableName)
				if metric, err := icebergTable.CollectMetrics(table); err != nil {
					log.Printf("Error collecting metrics for %s.%s.%s: %v", table.Catalog, table.Schema, table.TableName, err)
				} else {
					// log.Printf("printing metric fqn %v , avgMB %v, totalGB %v, fileCount %v, snapshotCount %v\n", metric.FQN, metric.AvgMB, metric.TotalGB, metric.FileCount, metric.SnapshotCount)
					metrics = append(metrics, metric)
				}
			}
		}(i)
	}
	for _, table := range tables {
		jobs <- table
	}
	close(jobs)
	wg.Wait()
	return metrics
}

func (c *TrinoClient) WriteMetricsToDB(results []*types.TableMetric) {
	if len(results) == 0 {
		return
	}

	// Build a query: INSERT INTO ... VALUES (?,?,?), (?,?,?), ...
	valueStrings := make([]string, 0, len(results))
	valueArgs := make([]any, 0, len(results)*6)

	for _, m := range results {
		// We explicitly cast EVERY column to match the DDL exactly
		placeholders := `(
            CAST(? AS VARCHAR), 
            CAST(? AS BIGINT), 
            CAST(? AS DOUBLE), 
            CAST(? AS DOUBLE), 
            CAST(? AS DOUBLE), 
            CAST(? AS BIGINT), 
            FROM_ISO8601_TIMESTAMP(?)
        )`
		valueStrings = append(valueStrings, placeholders)

		// Pass values as strings or their native types; CAST handles the rest
		valueArgs = append(valueArgs,
			m.FQN,
			m.FileCount.Int64, // Access the Int64 field of sql.NullInt64
			fmt.Sprintf("%f", m.AvgMB.Float64),
			fmt.Sprintf("%f", m.MedianMB.Float64), // Don't forget the median!
			fmt.Sprintf("%f", m.TotalGB.Float64),
			m.SnapshotCount.Int64,
			m.CollectedAt.Format(time.RFC3339), // "2026-03-29T07:56:11Z"
		)
	}

	query := fmt.Sprintf("INSERT INTO lakekeeper.crucible_admin.table_metrics_history VALUES %s",
		strings.Join(valueStrings, ","))

	_, err := c.DB.Exec(query, valueArgs...)
	if err != nil {
		log.Fatalf("Batch insert failed: %v", err)
	}
}
