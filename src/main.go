package main

import (
	"context"
	"crucible-go/src/client"
	"crucible-go/src/types"
	"log"

	_ "github.com/trinodb/trino-go-client/trino"
)

func loadConfig() (*types.CrucibleConfig, error) {
	cfg, err := LoadCrucibleConfig("config.yaml")
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func newTrinoClient(dsn string) (*client.TrinoClient, error) {
	dbClient, err := client.NewTrinoClient(dsn)
	if err != nil {
		return nil, err
	}
	return dbClient, nil
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Printf("Error loading crucible config: %v", err)
		return
	}

	dbClient, err := newTrinoClient(cfg.TrinoDSN)
	if err != nil {
		log.Fatalf("Error creating Trino client: %v", err)
	}
	defer dbClient.Close()
	ctx := context.Background()
	metrics := dbClient.ScanTableMetrics(ctx, cfg.Catalog, cfg.Workers)
	dbClient.WriteMetricsToDB(metrics)
}
