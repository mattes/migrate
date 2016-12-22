package sqlite3

import (
	"database/sql"
	"testing"

	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

// TestMigrate runs some additional tests on Migrate()
// Basic testing is already done in migrate/migrate_test.go
func TestMigrate(t *testing.T) {
	driverFile := ":memory:"
	driverUrl := "sqlite3://" + driverFile

	// prepare clean database
	connection, err := sql.Open("sqlite3", driverFile)
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
					id INTEGER PRIMARY KEY AUTOINCREMENT
				);
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "001_foobar.down.sql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
				DROP TABLE yolo;
			`),
		},
		{
			Path:      "/foobar",
			FileName:  "20060102150405_bigint.up.sql",
			Version:   20060102150405,
			Name:      "bigint",
			Direction: direction.Up,
			Content: []byte(`
               ALTER TABLE yolo ADD COLUMN okay text;
            `),
		},
		{
			Path:      "/foobar",
			FileName:  "20070000000000_foobar.up.sql",
			Version:   20070000000000,
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
		t.Error("Expected test case to fail")
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
