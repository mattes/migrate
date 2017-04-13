package firebirdsql

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
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	driverURL := "migrateuser:migratepass@firebirdsql:3050/databases/migratedb"

	// prepare clean database
	connection, err := sql.Open("firebirdsql", driverURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(`
					EXECUTE BLOCK AS BEGIN
						IF (EXISTS(SELECT 1 FROM rdb$relations WHERE rdb$relation_name = 'YOLO'))
						THEN BEGIN
							EXECUTE STATEMENT 'DROP TABLE YOLO;';
						END
						IF (EXISTS(SELECT 1 FROM rdb$relations WHERE rdb$relation_name = '` + tableName + `'))
						THEN BEGIN
							EXECUTE STATEMENT 'DROP TABLE ` + tableName + `;';
						END
					END;`); err != nil {
		t.Fatal(err)
	}
	connection.Close()

	d := &Driver{}
	if err := d.Initialize(driverURL); err != nil {
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
	      CREATE TABLE YOLO (
	        id INTEGER, PRIMARY KEY (id)
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
	      DROP TABLE YOLO;
	    `),
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
	  t.Error("Expected test case to fail")
	}

	if err := d.Close(); err != nil {
	  t.Fatal(err)
	}
}
