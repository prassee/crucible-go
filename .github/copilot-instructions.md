# Copilot Instructions for crucible-go

## Project Overview
crucible-go is a Go application that scans Iceberg tables in a Trino catalog, collects file and snapshot metrics for each table using parallel workers, and stores the metrics in a Trino table for monitoring and analysis.

Go Version: 1.26.0

## Dependencies
- `github.com/apache/iceberg-go v0.5.0` - Iceberg Go client for table metadata, snapshots, and manifest operations
- `github.com/trinodb/trino-go-client v0.333.0` - Trino database driver
- `gopkg.in/yaml.v3 v3.0.1` - YAML config parsing
- `database/sql` (stdlib) - SQL database interface
- `context` (stdlib) - Context for cancellation/timeouts
- `github.com/apache/arrow-go/v18` (iceberg-go dependency) - Arrow data format

## Project Structure
```
crucible-go/
├── src/
│   ├── main.go           # Entry point, loads config, creates Trino client, orchestrates metrics collection
│   ├── config.go        # Loads YAML configuration
│   ├── client/
│   │   ├── TrinoClient.go    # Main Trino client (connection, metrics scanning, batch insert)
│   │   └── IcerbergTable.go # Iceberg table queries (GetTables, CollectMetrics)
│   └── types/
│       └── types.go    # Core data structures
├── config.yaml        # Configuration file
└── go.mod
```

## Core Functionality

### 1. Configuration (config.yaml)
```yaml
trino_dsn: "trino://trino-coordinator:8080/catalog"
catalog: " iceberg"       # Iceberg catalog to scan
workers: 4              # Number of parallel workers
```
Load config using `LoadCrucibleConfig(path)` from `src/config.go`.

### 2. Trino Client (src/client/TrinoClient.go)
- `NewTrinoClient(dsn)` - Creates a new Trino client connection
- `ScanTableMetrics(ctx, catalog, workersCount)` - Scans all tables in catalog using parallel workers, returns []*types.TableMetric
- `WriteMetricsToDB(results)` - Batch inserts metrics to lakekeeper.crucible_admin.table_metrics_history

### 3. Iceberg Table Queries (src/client/IcerbergTable.go)
- `GetTables(catalog)` - Returns all BASE TABLEs from information_schema (excludes information_schema and system schemas)
- `CollectMetrics(table)` - Collects metrics for a single table:
  - File count from `tableName$files`
  - Average file size (MB) from `tableName$files`
  - Total file size (GB) from `tableName$files`
  - Snapshot count from `tableName$snapshots`

### 4. Data Types (src/types/types.go)
- `CrucibleConfig` - Configuration struct (TrinoDSN, Catalog, Workers)
- `TableIdentifier` - Identifies a table (Catalog, Schema, TableName)
- `TableMetric` - Metrics for a table (FQN, FileCount, AvgMB, MedianMB, TotalGB, SnapshotCount, CollectedAt)
- `SnapShotRow` - Snapshot metadata
- `IbTableAggregate` - Aggregated table info

### 5. Output Table Schema
Metrics are written to: `lakekeeper.crucible_admin.table_metrics_history`
| Column | Type |
|-------|------|
| fqn | VARCHAR |
| file_count | BIGINT |
| avg_mb | DOUBLE |
| median_mb | DOUBLE |
| total_gb | DOUBLE |
| snapshot_count | BIGINT |
| collected_at | TIMESTAMP |

## Adding New Functionality

### Adding a New Metric
1. Add field to `TableMetric` in `src/types/types.go`
2. Add query in `CollectMetrics` (src/client/IcerbergTable.go)
3. Update batch insert in `WriteMetricsToDB` (src/client/TrinoClient.go)
4. Add column to output table DDL

### Adding a New Table Query
Follow `GetTables` pattern:
```go
func (t *IcebergTable) QueryMethod(params...) (result, error) {
    query := fmt.Sprintf(`SELECT ... FROM "%s"."%s"."%s$table"`, ...)
    row := t.DB.QueryRowContext(t.Ctx, query)
    // scan results
    return result, nil
}
```

### Using Iceberg-Go for Table Metadata
The iceberg-go library provides programmatic access to Iceberg table metadata. Use it for advanced operations:

```go
import (
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/catalog"
)

// Load a table using catalog
catalog, err := catalog.NewTrinoCatalog(ctx, "catalog_name", dsn)
table, err := catalog.LoadTable(ctx, "database.table_name")

// Read table metadata
snapshots := table.SnapshotRefs()
manifests, err := table.Manifests(ctx)

// Read data files
manifestEntries, err := manifests.ReadManifestEntries(ctx)
for _, entry := range manifestEntries {
    // entry.Status(), entry.DataFile().Path(), entry.DataFile().RecordCount(), etc.
}
```

Key iceberg-go types:
- `iceberg.Table` - Iceberg table handle
- `catalog.Catalog` - Catalog interface (Trino, Hive, Nessie)
- `catalog.NewTrinoCatalog()` - Create Trino catalog
- `table.SnapshotRefs()` - Get all snapshots
- `table.Manifests()` - Get manifest files
- `ManifestEntry` - Individual manifest entry with data file metadata

### Adding a New Worker Stage
In `ScanTableMetrics`, add another worker pool after the existing one:
```go
// Process results from first stage
for i := 1; i <= workersCount; i++ {
    wg.Add(1)
    go func(workerID int) {
        defer wg.Done()
        for result := range resultsCh {
            // process
            resultsCh2 <- processedResult
        }
    }(i)
}
```

## Conventions
- Follow standard Go formatting (go fmt)
- Use meaningful variable names
- Handle errors explicitly, avoid bare `_` ignores
- Group imports: stdlib, then external packages
- Use context for cancellation/timeouts
- Use sync.WaitGroup for worker pools

## Testing
- Run `go test ./...` to execute tests

## Build & Run
- Build: `go build -o crucible-go ./src`
- Run: `go run ./src`