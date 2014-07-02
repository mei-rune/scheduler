package main

import (
	"database/sql"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

func backendTest(t *testing.T, cb func(backend *dbBackend)) {
	old_mode := *run_mode
	*run_mode = "init_db"
	defer func() {
		*run_mode = old_mode
	}()
	e := Main()
	if nil != e {
		t.Error(e)
		return
	}

	backend, e := newBackend(*db_drv, *db_url, nil)
	if nil != e {
		t.Error(e)
		return
	}
	defer backend.Close()

	// 	_, e = backend.db.Exec(`
	// DROP TABLE IF EXISTS ` + *table_name + `;

	// CREATE TABLE IF NOT EXISTS ` + *table_name + ` (
	//   id                SERIAL  PRIMARY KEY,
	//   priority          int DEFAULT 0,
	//   attempts          int DEFAULT 0,
	//   queue             varchar(200),
	//   handler           text  NOT NULL,
	//   handler_id        varchar(200),
	//   last_error        varchar(2000),
	//   run_at            timestamp,
	//   locked_at         timestamp,
	//   failed_at         timestamp,
	//   locked_by         varchar(200),
	//   created_at        timestamp NOT NULL,
	//   updated_at        timestamp NOT NULL
	// );`)
	// 	if nil != e {
	// 		t.Error(e)
	// 		return
	// 	}
	cb(backend)
}
