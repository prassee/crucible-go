package main

import (
	"database/sql"
	"fmt"

	"crucible-go/src/types"

	_ "github.com/trinodb/trino-go-client/trino"
)

func main() {
	dsn := "http://admin@localhost:9080?catalog=default&schema=test"
	db, err := sql.Open("trino", dsn)
	if err != nil {
		fmt.Println("Error connecting to Trino:", err)
		return
	}
	defer db.Close()

	tables, err := LoadTablesConfig("config.yaml")
	if err != nil {
		fmt.Println("Error loading tables config:", err)
		return
	}

	for _, table := range tables {
		fmt.Printf("Processing table: %s.%s.%s\n", table.Catalog, table.Schema, table.TableName)
		collectSnapshotMetrics(table, db)
	}
}

func collectSnapshotMetrics(table types.TableIdentifier, db *sql.DB) {
	query := fmt.Sprintf("select * from \"%s\".\"%s\".\"%s$history\"", table.Catalog, table.Schema, table.TableName)
	rows, err := db.Query(query)
	if err != nil {
		fmt.Println("Error in query:", err)
		return
	}
	for rows.Next() {
		var row types.SnapShotRow
		if err := rows.Scan(&row.MadeCurrentAt, &row.SnapShotID, &row.ParentID, &row.IsCurrentAncestor); err != nil {
			fmt.Println("Error scanning row:", err)
			continue
		}
		fmt.Printf("SnapShotID: %d, ParentID: %d, IsCurrentAncestor: %v, MadeCurrentAt: %s\n",
			row.SnapShotID, row.ParentID, row.IsCurrentAncestor, row.MadeCurrentAt)
	}
	defer rows.Close()
}
