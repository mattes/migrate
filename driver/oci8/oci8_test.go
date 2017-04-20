package oci8

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

func executeIfTableExistsPrep(table, query string) string {
	return fmt.Sprintf(`begin
	if table_exists('%s') = 1 then
	execute immediate 
	'%s';
	end if;
	end;`, table, query)
}

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	dsn := os.Getenv("OCI8_DB_DSN")

	if dsn == "" {
		t.Fatal("OCI8_DB_DSN environment variable not set, format (system/oracle@localhost:49161/xe)")
	}

	driverUrl := "oci8://" + dsn

	db, err := sql.Open("oci8", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a table_exists function in the database.
	_, err = db.Exec(createTableExistsFunc)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(executeIfTableExistsPrep("yolo", "DROP TABLE yolo"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(executeIfTableExistsPrep("yolo", "DROP TABLE "+tableName))
	if err != nil {
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
					id NUMBER(19) PRIMARY KEY
				)
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.down.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content:   []byte("DROP TABLE yolo"),
		},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.up.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
				CREATE TABLE error (
					THIS; WILL CAUSE; AN ERROR;
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
		t.Error("expected test case to fail")
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
