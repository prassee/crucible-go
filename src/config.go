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

	var configs []types.TableConfig
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&configs); err != nil {
		return nil, err
	}

	tables := make([]types.TableIdentifier, len(configs))
	for i, c := range configs {
		tables[i] = types.TableIdentifier{
			Catalog:   c.Catalog,
			Schema:    c.Schema,
			TableName: c.TableName,
		}
	}
	return tables, nil
}
