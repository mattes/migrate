package cassandra

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/heetch/migrate/file"
	"github.com/heetch/migrate/migrate/direction"
)

type Driver struct {
	session *gocql.Session
}

const (
	tableName  = "schema_migrations"
	driverName = "cassandra"
)

func (driver *Driver) Initialize(rawurl string) error {
	u, err := url.Parse(rawurl)

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	// Check if url user struct is null
	if u.User != nil {
		password, passwordSet := u.User.Password()

		if passwordSet == false {
			return fmt.Errorf("Missing password. Please provide password.")
		}

		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: u.User.Username(),
			Password: password,
		}

	}

	driver.session, err = cluster.CreateSession()

	if err != nil {
		return err
	}

	if err := driver.ensureVersionTableExists(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) Close() error {
	driver.session.Close()
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {

	if err := driver.session.Query("CREATE TABLE IF NOT EXISTS " + tableName +
		" (driver_name text," +
		"version bigint," +
		"file_name text," +
		"applied_at timestamp," +
		"PRIMARY KEY (driver_name,  version)" +
		") WITH CLUSTERING ORDER BY (version DESC);").Exec(); err != nil {
		return err
	}
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "cql"
}

func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	for _, query := range strings.Split(string(f.Content), ";") {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}
		if err := driver.session.Query(query).Exec(); err != nil {
			pipe <- err
			return
		}
	}

	if f.Direction == direction.Up {
		if err := driver.session.Query("INSERT INTO "+tableName+" (driver_name, version, file_name, applied_at)"+
			" VALUES (?, ?, ?, dateof(now()))", driverName, f.Version, f.FileName).Exec(); err != nil {
			pipe <- err
			return
		}
	} else if f.Direction == direction.Down {
		if err := driver.session.Query("DELETE FROM "+tableName+" WHERE driver_name=? AND version=?", driverName, f.Version).Exec(); err != nil {
			pipe <- err
			return
		}
	}
}

func (driver *Driver) Version() (uint64, error) {
	var version uint64
	err := driver.session.Query("SELECT version FROM "+tableName+" WHERE driver_name=? ORDER BY version DESC LIMIT 1", driverName).Scan(&version)
	if err != nil && err.Error() == "not found" {
		return 0, nil
	}
	return version, err
}
