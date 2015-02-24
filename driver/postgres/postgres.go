// Package postgres implements the PerFileTxnDriver interface.
package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/fedyakin/migrate/file"
	"github.com/fedyakin/migrate/migrate/direction"
	"github.com/lib/pq"
)

type PerFileTxnDriver struct {
	db *sql.DB
}

type NoTxnDriver struct {
	PerFileTxnDriver
}

type SingleTxnDriver struct {
	PerFileTxnDriver
	rollback bool
	txn      *sql.Tx
}

const tableName = "schema_migrations"

func (driver *PerFileTxnDriver) Initialize(url string) error {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	driver.db = db

	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *PerFileTxnDriver) Close() error {
	if err := driver.db.Close(); err != nil {
		return err
	}
	return nil
}

func (driver *PerFileTxnDriver) ensureVersionTableExists() error {
	if _, err := driver.db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (version int not null primary key);"); err != nil {
		return err
	}
	return nil
}

func (driver *PerFileTxnDriver) FilenameExtension() string {
	return "sql"
}

func (driver *PerFileTxnDriver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	tx, err := driver.db.Begin()
	if err != nil {
		pipe <- err
		return
	}

	// Don't update the version number to count always run files
	if f.Always == false {
		if f.Direction == direction.Up {
			if _, err := tx.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", f.Version); err != nil {
				pipe <- err
				if err := tx.Rollback(); err != nil {
					pipe <- err
				}
				return
			}
		} else if f.Direction == direction.Down {
			if _, err := tx.Exec("DELETE FROM "+tableName+" WHERE version=$1", f.Version); err != nil {
				pipe <- err
				if err := tx.Rollback(); err != nil {
					pipe <- err
				}
				return
			}
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		if err := tx.Rollback(); err != nil {
			pipe <- err
		}
		return
	}

	if _, err := tx.Exec(string(f.Content)); err != nil {
		pqErr := err.(*pq.Error)
		offset, err := strconv.Atoi(pqErr.Position)
		if err == nil && offset >= 0 {
			lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
			errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
			pipe <- errors.New(fmt.Sprintf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart)))
		} else {
			pipe <- errors.New(fmt.Sprintf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message))
		}

		if err := tx.Rollback(); err != nil {
			pipe <- err
		}
		return
	}

	if err := tx.Commit(); err != nil {
		pipe <- err
		return
	}
}

func (driver *PerFileTxnDriver) Version() (uint64, error) {
	var version uint64
	err := driver.db.QueryRow("SELECT version FROM " + tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}

func (driver *NoTxnDriver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	// Don't update the version number to count always run files
	if f.Always == false {
		if f.Direction == direction.Up {
			if _, err := driver.db.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", f.Version); err != nil {
				pipe <- err
				return
			}
		} else if f.Direction == direction.Down {
			if _, err := driver.db.Exec("DELETE FROM "+tableName+" WHERE version=$1", f.Version); err != nil {
				pipe <- err
				return
			}
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	if _, err := driver.db.Exec(string(f.Content)); err != nil {
		pqErr := err.(*pq.Error)
		offset, err := strconv.Atoi(pqErr.Position)
		if err == nil && offset >= 0 {
			lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
			errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
			pipe <- errors.New(fmt.Sprintf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart)))
		} else {
			pipe <- errors.New(fmt.Sprintf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message))
		}

		return
	}
}

func (driver *SingleTxnDriver) Initialize(url string) error {
	err := driver.PerFileTxnDriver.Initialize(url)
	if err != nil {
		return err
	}

	driver.txn, err = driver.db.Begin()
	return err
}

func (driver *SingleTxnDriver) Close() error {
	var err error
	if driver.rollback {
		err = driver.txn.Rollback()
	} else {
		err = driver.txn.Commit()
	}

	if err != nil {
		driver.PerFileTxnDriver.Close()
		return err
	}

	return driver.PerFileTxnDriver.Close()
}

func (driver *SingleTxnDriver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	// Don't update the version number to count always run files
	if f.Always == false {
		if f.Direction == direction.Up {
			if _, err := driver.txn.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", f.Version); err != nil {
				pipe <- err
				driver.rollback = true
				return
			}
		} else if f.Direction == direction.Down {
			if _, err := driver.txn.Exec("DELETE FROM "+tableName+" WHERE version=$1", f.Version); err != nil {
				pipe <- err
				driver.rollback = true
				return
			}
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		driver.rollback = true
		return
	}

	if _, err := driver.txn.Exec(string(f.Content)); err != nil {
		pqErr := err.(*pq.Error)
		offset, err := strconv.Atoi(pqErr.Position)
		if err == nil && offset >= 0 {
			lineNo, columnNo := file.LineColumnFromOffset(f.Content, offset-1)
			errorPart := file.LinesBeforeAndAfter(f.Content, lineNo, 5, 5, true)
			pipe <- errors.New(fmt.Sprintf("%s %v: %s in line %v, column %v:\n\n%s", pqErr.Severity, pqErr.Code, pqErr.Message, lineNo, columnNo, string(errorPart)))
		} else {
			pipe <- errors.New(fmt.Sprintf("%s %v: %s", pqErr.Severity, pqErr.Code, pqErr.Message))
		}

		driver.rollback = true
		return
	}
}
