package main

import (
	"crucible-go/src/types"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadTablesConfig(path string) ([]types.TableIdentifier, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var tables types.Tables
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&tables); err != nil {
		return nil, err
	}

	var result []types.TableIdentifier
	for _, t := range tables {
		result = append(result, types.TableIdentifier{
			Catalog:   t.Catalog,
			Schema:    t.Schema,
			TableName: t.TableName,
		})
	}
	return result, nil
}
