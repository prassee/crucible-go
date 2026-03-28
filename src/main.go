package main

import (
	"context"
	"crucible-go/src/client"
	"fmt"
	"log"

	_ "github.com/trinodb/trino-go-client/trino"
)

func main() {
	dbClient, err := client.NewTrinoClient("http://admin@localhost:9080?catalog=default&schema=test")
	if err != nil {
		log.Fatalf("Error creating Trino client: %v", err)
	}
	defer dbClient.Close()
	tables, err := LoadTablesConfig("config.yaml")
	if err != nil {
		log.Fatalf("Error loading tables config: %v", err)
	}

	ctx := context.Background()
	icebergTable := &client.IcebergTable{Ctx: ctx, DB: dbClient.DB}
	for _, table := range tables {
		fmt.Printf("Processing table: %s.%s.%s\n", table.Catalog, table.Schema, table.TableName)
		if _, err := icebergTable.CollectMetrics(table); err != nil {
			log.Fatalf("Error collecting snapshot metrics for %s.%s.%s: %v", table.Catalog, table.Schema, table.TableName, err)
		}
	}
}
