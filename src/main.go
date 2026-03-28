package main

import (
	"database/sql"
	"fmt"

	_ "github.com/trinodb/trino-go-client/trino"
)

type SnapShotRow struct {
	MadeCurrentAt     string
	SnapShotID        int64
	ParentID          int64
	IsCurrentAncestor bool
}

func main() {
	dsn := "http://admin@localhost:9080?catalog=default&schema=test"
	db, err := sql.Open("trino", dsn)
	if err != nil {
		fmt.Println("Error connecting to Trino:", err)
		return
	}
	err = db.Ping()
	if err == nil {
		fmt.Println("can ping database")
	}
	rows, err := db.Query("select * from \"lakekeeper\".\"uam_sync_uam_public\".\"user_activity$history\"")
	if err != nil {
		fmt.Println("Error in query:", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var row SnapShotRow
			if err := rows.Scan(&row.MadeCurrentAt, &row.SnapShotID, &row.ParentID, &row.IsCurrentAncestor); err != nil {
				fmt.Println("Error scanning row:", err)
				continue
			}
			fmt.Printf("SnapShotID: %d, ParentID: %d, IsCurrentAncestor: %v, MadeCurrentAt: %s\n",
				row.SnapShotID, row.ParentID, row.IsCurrentAncestor, row.MadeCurrentAt)
		}
	}
	defer db.Close()

}
