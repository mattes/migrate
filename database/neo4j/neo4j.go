package neo4j

import (
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"strings"

	"database/sql"
	_ "gopkg.in/cq.v1"
	"github.com/mattes/migrate"
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
	db       *sql.DB
	tx       *sql.Tx
	isLocked bool
	config   *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
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
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("neo4j-cypher", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	migrationsLabel := purl.Query().Get("x-migrations-label")
	if len(migrationsLabel) == 0 {
		migrationsLabel = DefaultMigrationsLabel
	}

	mx, err := WithInstance(db, &Config{
		MigrationsLabel: migrationsLabel,
	})
	if err != nil {
		return nil, err
	}

	return mx, nil
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

		stmt, err := m.db.Prepare(query)
		if err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		defer stmt.Close()

		if _, err := stmt.Exec(); err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Err: "migration failed", Query: []byte(query)}
		}
	}

	return nil
}

func (m *Neo4j) SetVersion(version int, dirty bool) error {

	query := "MATCH (m:" + m.config.MigrationsLabel + ") delete m"
	stmt, err := m.db.Prepare(query)
	if err != nil {
		m.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer stmt.Close()

	if _, err := stmt.Exec(version); err != nil {
		m.Rollback()
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if version >= 0 {

		query := "MATCH (m:" + m.config.MigrationsLabel + ") where m.version={0} delete m"
		stmt, err := m.db.Prepare(query)
		if err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		defer stmt.Close()

		if _, err := stmt.Exec(version); err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		query = "CREATE (:" + m.config.MigrationsLabel + " {version:{0}, dirty:{1}})"
		stmt, err = m.db.Prepare(query)
		if err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		defer stmt.Close()
		if _, err := stmt.Exec(version, dirty); err != nil {
			m.Rollback()
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	return nil
}

func (m *Neo4j) Version() (version int, dirty bool, err error) {
	query := "MATCH (m:" + m.config.MigrationsLabel + ") return m.version, m.dirty ORDER BY m.version LIMIT 1"
	stmt, err := m.db.Prepare(query)
	if err != nil {
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer stmt.Close()
	err = stmt.QueryRow(query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return int(version), dirty, nil
	}
}

func (m *Neo4j) Drop() error {
	// select all tables
	query := "MATCH (m:" + m.config.MigrationsLabel + ") delete m"
	stmt, err := m.db.Prepare(query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
