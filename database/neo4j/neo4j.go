package neo4j

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/mattes/migrate/database"
)

func init() {
	database.Register("neo4j", &Neo4j{})
}

var DefaultMigrationsLabel = "SchemaMigration"

var (
	ErrNilConfig = fmt.Errorf("no config")
)

type Config struct {
	MigrationsLabel string
}

type Neo4j struct {
	db       bolt.Conn
	tx       bolt.Tx
	isLocked bool
	config   *Config
}

func WithInstance(instance bolt.Conn, config *Config) (database.Driver, error) {
	if instance == nil || config == nil {
		return nil, ErrNilConfig
	}

	if len(config.MigrationsLabel) == 0 {
		config.MigrationsLabel = DefaultMigrationsLabel
	}

	mx := &Neo4j{
		db:     instance,
		config: config,
	}

	return mx, nil
}

func (m *Neo4j) Open(url string) (database.Driver, error) {
	boltDriver := bolt.NewDriver()
	conn, err := boltDriver.OpenNeo(url)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	driver, err := WithInstance(conn, &Config{})
	if err != nil {
		return nil, err
	}
	return driver, nil
}

func (m *Neo4j) Close() error {
	return m.db.Close()
}

func (m *Neo4j) Lock() error {
	if m.isLocked {
		return database.ErrLocked
	}
	tx, err := m.db.Begin()
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}
	m.tx = tx
	m.isLocked = true
	return nil
}

func (m *Neo4j) Unlock() (err error) {
	m.isLocked = false
	if m.tx != nil {
		if e := m.tx.Commit(); e != nil {
			err = &database.Error{OrigErr: err, Err: "transaction commit failed"}
		}
		m.tx = nil
	}
	return
}

func (m *Neo4j) Rollback() (err error) {
	if m.tx != nil {
		if e := m.tx.Rollback(); e != nil {
			err = &database.Error{OrigErr: err, Err: "transaction rollback failed"}
		}
		m.tx = nil
	}
	return
}

func (m *Neo4j) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	contents := string(migr[:])
	queries := strings.Split(contents, ";\n")

	for _, query := range queries {

		if len(strings.TrimSpace(query)) == 0 {
			continue
		}

		stmt, err := m.db.PrepareNeo(query)
		if err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		defer stmt.Close()

		if _, err := stmt.ExecNeo(nil); err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(query)}
		}
		stmt.Close()
	}

	return nil
}

func (m *Neo4j) SetVersion(version int, dirty bool) error {

	query := "MATCH (m:" + m.config.MigrationsLabel + ") delete m"
	stmt1, err := m.db.PrepareNeo(query)
	if err != nil {
		m.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer stmt1.Close()

	if _, err := stmt1.ExecNeo(map[string]interface{}{}); err != nil {
		m.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	stmt1.Close()

	if version >= 0 {

		query := "MATCH (m:" + m.config.MigrationsLabel + ") where m.version={version} delete m"
		stmt2, err := m.db.PrepareNeo(query)
		if err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		defer stmt2.Close()

		if _, err := stmt2.ExecNeo(map[string]interface{}{"version": version}); err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		stmt2.Close()

		query = "CREATE (:" + m.config.MigrationsLabel + " {version:{version}, dirty:{dirty}})"
		stmt3, err := m.db.PrepareNeo(query)
		if err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		defer stmt3.Close()
		if _, err := stmt3.ExecNeo(map[string]interface{}{"version": version, "dirty": dirty}); err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		stmt3.Close()
	}

	return nil
}

func (m *Neo4j) Version() (version int, dirty bool, err error) {
	query := "MATCH (m:" + m.config.MigrationsLabel + ") return m.version, m.dirty ORDER BY m.version LIMIT 1"
	stmt, err := m.db.PrepareNeo(query)
	if err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer stmt.Close()
	rows, err := stmt.QueryNeo(nil)
	data, _, err := rows.NextNeo()
	if err != nil {
		if err == io.EOF {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return data[0].(int), data[1].(bool), nil

}

func (m *Neo4j) Drop() error {
	// select all tables
	query := "MATCH (m:" + m.config.MigrationsLabel + ") delete m"
	stmt, err := m.db.PrepareNeo(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer stmt.Close()
	_, err = stmt.ExecNeo(nil)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
