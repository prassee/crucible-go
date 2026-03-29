package main

import (
	"context"
	"crucible-go/src/client"
	"log"

	_ "github.com/trinodb/trino-go-client/trino"
)

func main() {
	// Read config.yaml
	cfg, err := LoadCrucibleConfig("config.yaml")
	if err != nil {
		log.Printf("Error loading crucible config: %v", err)
		return
	}

	dbClient, err := client.NewTrinoClient(cfg.TrinoDSN)
	if err != nil {
		log.Fatalf("Error creating Trino client: %v", err)
	}
	defer dbClient.Close()
	ctx := context.Background()
	metrics := dbClient.ScanTableMetrics(ctx, cfg.Catalog, cfg.Workers)
	dbClient.WriteMetricsToDB(metrics)
}
