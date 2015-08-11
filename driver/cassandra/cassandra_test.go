package cassandra

import (
	"net/url"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/heetch/migrate/file"
	"github.com/heetch/migrate/migrate/direction"
	pipep "github.com/heetch/migrate/pipe"
)

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
			FileName:  "20150801233454_foobar.up.sql",
			Version:   20150801233454,
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
			FileName:  "20150801233454_foobar.down.sql",
			Version:   20150801233454,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
                DROP TABLE yolo;
            `),
		},
		{
			Path:      "/foobar",
			FileName:  "20150803233454_foobar.up.sql",
			Version:   20150803233454,
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
	if version != 20150801233454 {
		t.Fatal("Unable to migrate up. Expected 20150801233454 but get ", version)
	}

	pipe = pipep.New()
	go d.Migrate(files[1], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	version, err = d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if version != 0 {
		t.Fatal("Unable to migrate down. Expected 0 but get ", version)
	}

	pipe = pipep.New()
	go d.Migrate(files[2], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}

	version, err = d.Version()
	if err != nil {
		t.Fatal(err)
	}
	if version != 0 {
		t.Fatal("This migration should have failed, so we should get the last migration version 0 in here.", version)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

}
