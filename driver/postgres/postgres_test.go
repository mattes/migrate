package postgres

import (
	"database/sql"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
	"testing"
	"io/ioutil"
	"bytes"
)

// TestMigrate runs some additional tests on Migrate().
// Basic testing is already done in migrate/migrate_test.go
func TestMigrate(t *testing.T) {
	driverUrl := "postgres://localhost/migratetest?sslmode=disable"

	// prepare clean database
	connection, err := sql.Open("postgres", driverUrl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(`
				DROP TABLE IF EXISTS yolo;
				DROP TABLE IF EXISTS ` + tableName + `;`); err != nil {
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
					id serial not null primary key
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

func TestDump(t *testing.T) {
	driverUrl := "postgres://localhost/migratetest?sslmode=disable"

	// prepare database with a couple tables
	connection, err := sql.Open("postgres", driverUrl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(`
			DROP TABLE IF EXISTS foos;
			CREATE TABLE foos (foo_id INTEGER, name TEXT); 
			DROP TABLE IF EXISTS bars;
			CREATE TABLE bars (bar_id INTEGER, created_on DATE); 
		`); err != nil {
		t.Fatal(err)
	}

	d := &Driver{}
	if err := d.Initialize(driverUrl); err != nil {
		t.Fatal(err)
	}

	// dump the database to a test file
	if err := d.Dump("/tmp/test.sql", nil); err != nil {
		t.Fatal(err)
	}

	// assert it looks how we expect it to look
	contents, err := ioutil.ReadFile("/tmp/test.sql")
	if err != nil {
	  t.Fatal(err)
	}

	if !bytes.Contains(contents, []byte("CREATE TABLE foos")) ||
		!bytes.Contains(contents, []byte("CREATE TABLE bars")) {
		t.Log(contents)
		t.Error("Expected dump file to contain CREATE TABLE statements for 'foos' and 'bars'; didn't find them.")
	}

	// now dump it without the bars table
  options := make(map[string]interface{})
	tables  := make([]string, 1)
	tables[0] = "bars"
	options["exclude_tables"] = tables

	if err := d.Dump("/tmp/test.sql", &options); err != nil {
		t.Fatal(err)
	}

	// assert it has no bars
	contents, err = ioutil.ReadFile("/tmp/test.sql")
	if err != nil {
	  t.Fatal(err)
	}

	if bytes.Contains(contents, []byte("CREATE TABLE bars")) {
		t.Log(string(contents))
		t.Error("Expected dump file to not contain CREATE TABLE statements for 'bars', but found one.")
	}
}

func TestLoad(t *testing.T) {
	driverUrl := "postgres://localhost/migratetest?sslmode=disable"

	// create a file for the driver to load
	if err := ioutil.WriteFile("/tmp/test-load.sql", []byte(`
			DROP TABLE IF EXISTS zeds;
			CREATE TABLE zeds (zed_id INTEGER);
		`), 0644); err != nil {
		t.Fatal(err)
	}

	d := &Driver{}
	if err := d.Initialize(driverUrl); err != nil {
		t.Fatal(err)
	}

	if err := d.Load("/tmp/test-load.sql"); err != nil {
		t.Fatal(err)
	}

	// make sure it worked
	connection, err := sql.Open("postgres", driverUrl)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := connection.Exec(`SELECT * FROM zeds;`); err != nil {
		t.Error("The schema was not loaded properly (can't query table 'zeds')")
		t.Fatal(err)
	}
}
