// Package neo4jbolt implements the Driver interface.
package neo4jbolt

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	log "log"
	"strings"

	bolt "github.com/axiomzen/golang-neo4j-bolt-driver"
	driver "github.com/axiomzen/migrate/driver"
	"github.com/axiomzen/migrate/file"
	"github.com/axiomzen/migrate/migrate/direction"
)

// Driver is the holding struct
type Driver struct {
	conn bolt.Conn
}

const labelName = "SchemaMigration"
const propertyName = "version"

// Initialize creates the db connection
func (d *Driver) Initialize(url string) error {
	op := bolt.DefaultDriverOptions()
	op.Addr = url
	dr := bolt.NewDriverWithOptions(op)
	conn, err := dr.OpenNeo()

	if err != nil {
		return err
	}

	d.conn = conn

	if err := d.ensureVersionConstraintExists(); err != nil {
		return err
	}
	return nil
}

// Close closes the db connection
func (d *Driver) Close() error {
	return d.conn.Close()
}

// FilenameExtension is the migration filename extension
func (d *Driver) FilenameExtension() string {
	return "cql"
}

func (d *Driver) inTx(fn func() (interface{}, error)) (interface{}, error) {
	tx, err := d.conn.Begin()
	if err != nil {
		return nil, err
	}
	var res interface{}
	if res, err = fn(); err != nil {
		// todo: what to do with rollback error?
		defer tx.Rollback()
		return nil, err
	}

	return res, tx.Commit()
}

func (d *Driver) ensureVersionConstraintExists() error {

	_, err := d.inTx(func() (interface{}, error) {
		stmt, err := d.conn.PrepareNeo(`CALL db.constraints()`)
		if err != nil {
			return nil, err
		}

		rows, err := stmt.QueryNeo(nil)
		if err != nil {
			return nil, err
		}

		constraint := fmt.Sprintf(
			"CONSTRAINT ON ( %s:%s ) ASSERT %s.%s IS UNIQUE",
			strings.ToLower(labelName),
			labelName,
			strings.ToLower(labelName),
			propertyName)

		// perhaps iterate this instead,
		// will have potentially a lot of constraints
		for {
			rowslice, _, err := rows.NextNeo()
			for _, row := range rowslice {
				if s, ok := row.(string); ok {
					if s == constraint {
						//log.Printf("We found constraint")
						return s, stmt.Close()
					}
				}
			}
			if err != nil {
				// exhausted the rows
				if err == io.EOF {
					break
				}
			} else {
				return nil, err
			}
		}

		// didn't find constraint
		err = stmt.Close()
		if err != nil {
			return nil, err
		}

		stmt, err = d.conn.PrepareNeo(fmt.Sprintf(`CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE`, labelName, propertyName))
		if err != nil {
			log.Printf("PrepareNeo index error: %s\n", err.Error())
			return nil, err
		}

		res, err := stmt.ExecNeo(nil)
		if err != nil {
			log.Printf("ExecNeo index error: %s\n", err.Error())
			return nil, err
		}

		if err = stmt.Close(); err != nil {
			return nil, err
		}
		return res, nil
	})
	if err != nil {
		log.Printf("ensureVersionConstraintExists index error: %s\n", err.Error())
	}
	return err
}

func (d *Driver) setVersion(dir direction.Direction, v uint64, invert bool) error {

	_, err := d.inTx(func() (interface{}, error) {
		cqUp := fmt.Sprintf(`CREATE (n:%s {version: %d}) RETURN n`, labelName, v)
		cqDown := fmt.Sprintf(`MATCH (n:%s {version: %d}) DELETE n`, labelName, v)
		var query string
		switch dir {
		case direction.Up:
			if invert {
				query = cqDown
			} else {
				query = cqUp
			}
		case direction.Down:
			if invert {
				query = cqUp
			} else {
				query = cqDown
			}
		}

		stmt, err := d.conn.PrepareNeo(query)
		if err != nil {
			return nil, err
		}

		res, err := stmt.ExecNeo(nil)
		if err != nil {
			return nil, err
		}
		if err = stmt.Close(); err != nil {
			return nil, err
		}
		return res, nil
	})

	return err
}

// Migrate performs the migration
func (d *Driver) Migrate(f file.File, pipe chan interface{}) {
	var err error

	defer func() {
		if err != nil {
			// Invert version direction if we couldn't apply the changes for some reason.
			if err := d.setVersion(f.Direction, f.Version, true); err != nil {
				pipe <- err
			}
			pipe <- err
		}
		close(pipe)
	}()

	pipe <- f

	// Neo4J: Cannot perform data updates in a transaction that has performed schema update :(
	// the migrations could perform schema updates, so the verison setting has to be done separatley

	// this wont be called if we are already on that version
	// however, I am sure there are race conditions that exist
	// as the version check and and updating should all be one transaction
	if err = d.setVersion(f.Direction, f.Version, false); err != nil {
		pipe <- err
		return
	}

	// read the file content
	if err = f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	var cQueries []string

	// split the statments into lines
	cqlStmts := bytes.Split(f.Content, []byte(";"))

	for _, cqlStmt := range cqlStmts {
		cqlStmt = bytes.TrimSpace(cqlStmt)
		if len(cqlStmt) > 0 {
			cQueries = append(cQueries, string(cqlStmt))
		}
	}

	// one statement at a time, but all in a tx
	_, err = d.inTx(func() (interface{}, error) {
		var results []bolt.Result
		for _, q := range cQueries {
			stmt, err := d.conn.PrepareNeo(q)
			if err != nil {
				log.Printf("error in d.conn.PrepareNeo, %s, %s\n", q, err)
				return nil, err
			}
			res, err := stmt.ExecNeo(nil)
			if err != nil {
				log.Printf("error in stmt.ExecNeo, %s\n", err)
				return nil, err
			}
			err = stmt.Close()
			if err != nil {
				log.Printf("error in stmt.Close(), %s\n", err)
				return nil, err
			}
			results = append(results, res)
		}
		return results, nil
	})

	if err != nil {
		pipe <- err
		return
	}
}

// Version gets the current migration version
func (d *Driver) Version() (uint64, error) {

	cq := fmt.Sprintf(
		`MATCH (n:%s) RETURN n.%s ORDER BY n.%s DESC LIMIT 1`,
		labelName,
		propertyName,
		propertyName)

	res, err := d.inTx(func() (interface{}, error) {
		stmt, err := d.conn.PrepareNeo(cq)
		if err != nil {
			return nil, err
		}
		rows, err := stmt.QueryNeo(nil)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()

		rowdata, _, err := rows.All()
		if err != nil {
			return nil, err
		}
		if len(rowdata) == 1 {
			if len(rowdata[0]) == 1 {
				if ver, vok := rowdata[0][0].(int64); vok {
					return uint64(ver), nil
				}
				log.Printf("rowdata was not a uint64, %#v\n", rowdata[0][0])
			} else {
				log.Printf("len of rowdata[0]: %d\n", len(rowdata[0]))
			}
		} else {
			//node doesn't exist yet, return 0
			return uint64(0), nil
		}
		return uint64(0), errors.New("bad row data")
	})

	if err != nil {
		return uint64(0), err
	}
	return res.(uint64), nil
}

func init() {
	driver.RegisterDriver("bolt", &Driver{})
}
