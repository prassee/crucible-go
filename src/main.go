package main

import (
	"context"
	"crucible-go/src/client"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/trinodb/trino-go-client/trino"
)

func main() {
	dsn := "http://admin@localhost:9080?catalog=default&schema=test"
	db, err := sql.Open("trino", dsn)
	if err != nil {
		log.Fatalf("Error connecting to Trino: %v", err)
	}
	defer db.Close()

	tables, err := LoadTablesConfig("config.yaml")
	if err != nil {
		log.Fatalf("Error loading tables config: %v", err)
	}

	ctx := context.Background()
	for _, table := range tables {
		fmt.Printf("Processing table: %s.%s.%s\n", table.Catalog, table.Schema, table.TableName)
		if err := client.CollectSnapshotMetrics(ctx, table, db); err != nil {
			log.Fatalf("Error collecting snapshot metrics for %s.%s.%s: %v", table.Catalog, table.Schema, table.TableName, err)
		}
	}
}
