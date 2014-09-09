// Package cassandra implements the Driver interface.
package cassandra

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
)

type Driver struct {
	session *gocql.Session
}

const tableName = "schema_migrations"
const versionRow = 1

// Cassandra Driver URL format:
// cassandra://host:port/keyspace[/protocol=2/cql=3.0.5]
//
// Example:
// cassandra://localhost/SpaceOfKeys
func (driver *Driver) Initialize(rawurl string) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		return err
	}

	cluster, err := clusterConfigFromUrl(u)
	if err != nil {
		return err
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

func clusterConfigFromUrl(u *url.URL) (*gocql.ClusterConfig, error) {
	// slashes are not valid in keyspace names, so we can use things after
	// the slash to further configure the connection; we lop off the leading
	// slash to start things off
	pathParts := strings.Split(u.Path[1:], "/")

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = pathParts[0]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	for _, part := range pathParts[1:] {
		kv := strings.Split(part, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("Invalid cassandra cluster config option %s", part)
		}
		key, value := kv[0], kv[1]
		switch key {
		case "protocol":
			numProto, err := strconv.ParseInt(value, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("Invalid protocol number: %s (%s)", value, err)
			}
			cluster.ProtoVersion = int(numProto)
		case "cql":
			cluster.CQLVersion = value
		default:
			return nil, fmt.Errorf("Invalid cassandra cluster config option %s", part)
		}
	}
	return cluster, nil
}

func (driver *Driver) Close() error {
	driver.session.Close()
	return nil
}

func (driver *Driver) ensureVersionTableExists() error {
	err := driver.session.Query("CREATE TABLE IF NOT EXISTS " + tableName + " (version counter, versionRow bigint primary key);").Exec()
	if err != nil {
		return err
	}

	_, err = driver.Version()
	if err != nil {
		driver.session.Query("UPDATE "+tableName+" SET version = version + 1 where versionRow = ?", versionRow).Exec()
	}

	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "cql"
}

func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f

	if f.Direction == direction.Up {
		err := driver.session.Query("UPDATE "+tableName+" SET version = version + 1 where versionRow = ?", versionRow).Exec()
		if err != nil {
			pipe <- err
			return
		}
	} else if f.Direction == direction.Down {
		err := driver.session.Query("UPDATE "+tableName+" SET version = version - 1 where versionRow = ?", versionRow).Exec()
		if err != nil {
			pipe <- err
			return
		}
	}

	if err := f.ReadContent(); err != nil {
		pipe <- err
		return
	}

	err := driver.session.Query(string(f.Content)).Exec()

	if err != nil {
		pipe <- err
		return
	}
}

func (driver *Driver) Version() (uint64, error) {
	var version int64
	err := driver.session.Query("SELECT version FROM "+tableName+" WHERE versionRow = ?", versionRow).Scan(&version)
	return uint64(version) - 1, err
}
