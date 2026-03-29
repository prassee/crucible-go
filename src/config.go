package main

import (
	"crucible-go/src/types"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadCrucibleConfig(path string) (types.CrucibleConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return types.CrucibleConfig{}, err
	}
	defer file.Close()

	var configs types.CrucibleConfig
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&configs); err != nil {
		return types.CrucibleConfig{}, err
	}
	return configs, nil
}
