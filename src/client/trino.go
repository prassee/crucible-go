package client

import "database/sql"

type TrinoClient struct {
	DSN string
	DB  *sql.DB
}

func NewTrinoClient(dsn string) (*TrinoClient, error) {
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, err
	}
	return &TrinoClient{DSN: dsn, DB: db}, nil
}

func (c *TrinoClient) Close() error {
	return c.DB.Close()
}
