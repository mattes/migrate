package cassandra

import (
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

func TestMigrate(t *testing.T) {
	var session *gocql.Session

	host := os.Getenv("CASSANDRA_PORT_9042_TCP_ADDR")
	port := os.Getenv("CASSANDRA_PORT_9042_TCP_PORT")
	driverUrl := "cassandra://" + host + ":" + port + "/system?protocol=4"

	// prepare a clean test database
	u, err := url.Parse(driverUrl)
	if err != nil {
		t.Fatal(err)
	}

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute
	cluster.ProtoVersion = 4

	session, err = cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}

	if err := resetKeySpace(session); err != nil {
		t.Fatal(err)
	}

	cluster.Keyspace = "migrate"
	session, err = cluster.CreateSession()
	driverUrl = "cassandra://" + host + ":" + port + "/migrate?protocol=4"

	d := &Driver{}
	if err := d.Initialize(driverUrl); err != nil {
		t.Fatal(err)
	}

	files := []file.File{
		{
			Path:      "/foobar",
			FileName:  "20060102150405_foobar.up.sql",
			Version:   20060102150405,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
                CREATE TABLE yolo (
                    id varint primary key,
                    msg text
                );

				CREATE INDEX ON yolo (msg);
            `),
		},
		{
			Path:      "/foobar",
			FileName:  "20060102150405_foobar.down.sql",
			Version:   20060102150405,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
                DROP TABLE yolo;
            `),
		},
		{
			Path:      "/foobar",
			FileName:  "20060102150406_foobar.up.sql",
			Version:   20060102150406,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
                CREATE TABLE error (
                    id THIS WILL CAUSE AN ERROR
                )
            `),
		},
	}

	pipe := pipep.New()
	go d.Migrate(files[0], pipe)
	errs := pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	version, err := d.Version()
	if err != nil {
		t.Fatal(err)
	}

	if version != 20060102150405 {
		t.Errorf("Expected version to be: %d, got: %d", 20060102150405, version)
	}

	// Check versions applied in DB
	expectedVersions := file.Versions{20060102150405}
	versions, err := d.Versions()
	if err != nil {
		t.Errorf("Could not fetch versions: %s", err)
	}

	if !reflect.DeepEqual(versions, expectedVersions) {
		t.Errorf("Expected versions to be: %v, got: %v", expectedVersions, versions)
	}

	pipe = pipep.New()
	go d.Migrate(files[1], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	pipe = pipep.New()
	go d.Migrate(files[2], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}

	// Check versions applied in DB
	expectedVersions = file.Versions{}
	versions, err = d.Versions()
	if err != nil {
		t.Errorf("Could not fetch versions: %s", err)
	}

	if !reflect.DeepEqual(versions, expectedVersions) {
		t.Errorf("Expected versions to be: %v, got: %v", expectedVersions, versions)
	}

	if err := resetKeySpace(session); err != nil {
		t.Fatal(err)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeReturnsErrorsForBadUrls(t *testing.T) {
	var session *gocql.Session

	host := os.Getenv("CASSANDRA_PORT_9042_TCP_ADDR")
	port := os.Getenv("CASSANDRA_PORT_9042_TCP_PORT")

	cluster := gocql.NewCluster(host)
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Query(`CREATE KEYSPACE IF NOT EXISTS migrate WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};`).Exec(); err != nil {
		t.Fatal(err)
	}

	d := &Driver{}
	invalidURL := "sdf://asdf://as?df?a"
	if err := d.Initialize(invalidURL); err == nil {
		t.Errorf("expected an error to be returned if url could not be parsed")
	}

	noKeyspace := "cassandra://" + host + ":" + port
	if err := d.Initialize(noKeyspace); err == nil {
		t.Errorf("expected an error to be returned if no keyspace provided")
	}
}

func resetKeySpace(session *gocql.Session) error {
	session.Query(`DROP KEYSPACE migrate;`).Exec()
	return session.Query(`CREATE KEYSPACE IF NOT EXISTS migrate WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};`).Exec()
}
