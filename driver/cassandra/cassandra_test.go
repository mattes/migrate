package cassandra

import (
	"net/url"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

func TestClusterConfigFromUrl(t *testing.T) {
	rawurl := "cassandra://localhos/migratetest?protocol=1&cql=3.0.1"
	u, err := url.Parse(rawurl)
	if err != nil {
		t.Fatal(err)
	}
	config, err := clusterConfigFromUrl(u)
	if err != nil {
		t.Fatal(err)
	}
	if config.ProtoVersion != 1 {
		t.Fatal("Protocol version is %d", config.ProtoVersion)
	}
	if config.CQLVersion != "3.0.1" {
		t.Fatal("CQL version is %s", config.CQLVersion)
	}
}

func TestDefaultsPreserved(t *testing.T) {
	cluster := gocql.NewCluster("localhost")
	rawurl := "cassandra://localhost/migratetest"
	u, err := url.Parse(rawurl)
	if err != nil {
		t.Fatal(err)
	}

	config, err := clusterConfigFromUrl(u)
	if err != nil {
		t.Fatal(err)
	}

	if config.ProtoVersion != cluster.ProtoVersion {
		t.Fatal("Protocol version was not left at default")
	}

	if config.CQLVersion != cluster.CQLVersion {
		t.Fatal("CQL version was not left at default")
	}
}

func TestInvalidConfigOptions(t *testing.T) {
	invalids := []string{
		"cassandra://localhost/migratetest/protocol=a",
		"cassandra://localhost/migratetest/proto=1/cql=3.0.1",
		"cassandra://localhost/migratetest/foo=bar",
		"cassandra://localhost/migratetest/proto/1",
	}
	for _, rawurl := range invalids {
		u, err := url.Parse(rawurl)
		if err != nil {
			t.Fatal(err)
		}
		_, err = clusterConfigFromUrl(u)
		if err == nil {
			t.Fatalf("We should have gotten an error and we did not from '%s'", rawurl)
		}
	}
}

func TestMigrate(t *testing.T) {
	var session *gocql.Session
	driverUrl := "cassandra://localhost/migratetest"

	// prepare a clean test database
	u, err := url.Parse(driverUrl)
	if err != nil {
		t.Fatal(err)
	}

	cluster := gocql.NewCluster(u.Host)
	cluster.Keyspace = u.Path[1:len(u.Path)]
	cluster.Consistency = gocql.All
	cluster.Timeout = 1 * time.Minute

	session, err = cluster.CreateSession()

	if err != nil {
		t.Fatal(err)
	}

	if err := session.Query(`DROP TABLE IF EXISTS yolo`).Exec(); err != nil {
		t.Fatal(err)
	}
	if err := session.Query(`DROP TABLE IF EXISTS ` + tableName).Exec(); err != nil {
		t.Fatal(err)
	}

	d := &Driver{}
	if err := d.Initialize(driverUrl); err != nil {
		t.Fatal(err)
	}

	files := []file.File{
		{
			Path:      "/foobar",
			FileName:  "001_foobar.up.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
                CREATE TABLE yolo (
                    id varint primary key
                );
            `),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.down.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
                DROP TABLE yolo;
            `),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.up.sql",
			Version:   2,
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

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

}
