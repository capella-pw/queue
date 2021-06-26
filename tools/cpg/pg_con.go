package main

import (
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	PGDriver = "postgres"
)

type PGConnection struct {
	ConnectionString  string        `json:"connection_string"`
	MaxConections     int           `json:"max_con"`
	MaxIdleConections int           `json:"max_idle_con"`
	MaxIdleTime       time.Duration `json:"max_idle_time"`

	db *sqlx.DB
}

func (pgc *PGConnection) Init() error {
	db, err := sqlx.Open(PGDriver, pgc.ConnectionString)

	if err != nil {
		return err
	}

	db.SetMaxIdleConns(pgc.MaxIdleConections)
	db.SetMaxOpenConns(pgc.MaxConections)
	db.SetConnMaxIdleTime(pgc.MaxIdleTime)

	pgc.db = db
	return nil
}
