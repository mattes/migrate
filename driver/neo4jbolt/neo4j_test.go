package neo4jbolt

import (
	"testing"

	"github.com/axiomzen/migrate/driver"
	"github.com/axiomzen/migrate/file"
	"github.com/axiomzen/migrate/migrate/direction"
	pipep "github.com/axiomzen/migrate/pipe"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

// TestMigrate runs some additional tests on Migrate().
// Basic testing is already done in migrate/migrate_test.go
func TestMigrate(t *testing.T) {

	// note: if you want to use the front end as well, change the
	// password to whatever you change it to in the web client
	driverURL := `bolt://neo4j:test@bolt:7687` // + host + ":" + port

	// prepare clean database
	tempdriver := bolt.NewDriver()
	conn, err := tempdriver.OpenNeo(driverURL)
	if err != nil {
		t.Fatal(err)
	}

	// cleanup tests
	// If an error dropping the index then ignore it
	conn.ExecPipeline([]string{`DROP INDEX ON :Yolo(name)`, `MATCH (n:` + labelName + `) DELETE n`})

	conn.Close()

	d0 := driver.GetDriver("bolt")
	d, ok := d0.(*Driver)
	if !ok {
		t.Fatal("Neo4JBolt driver has not registered")
	}

	if err := d.Initialize(driverURL); err != nil {
		t.Fatal(err)
	}

	files := []file.File{
		{
			Path:      "/foobar",
			FileName:  "001_foobar.up.cql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
        CREATE INDEX ON :Yolo(name)
      `)},
		{
			Path:      "/foobar",
			FileName:  "001_foobar.down.cql",
			Version:   1,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
        DROP INDEX ON :Yolo(name)
      `)},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.up.cql",
			Version:   2,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
        CREATE INDEX :Yolo(name) THIS WILL CAUSE AN ERROR
      `)},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.up.cql",
			Version:   2,
			Name:      "foobar",
			Direction: direction.Up,
			Content: []byte(`
        CREATE (n:Yolo {name: 'hithere', bar: 'bar'});
		CREATE (n:Yolo {name: 'byethere', foo: 'foo'});
      `)},
		{
			Path:      "/foobar",
			FileName:  "002_foobar.down.cql",
			Version:   2,
			Name:      "foobar",
			Direction: direction.Down,
			Content: []byte(`
        MATCH (n:Yolo {name: 'hithere'}) DELETE n;
		MATCH (n:Yolo {name: 'byethere'}) DELETE n;
      `)},
	}

	pipe := pipep.New()
	go d.Migrate(files[0], pipe)
	errs := pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// check version
	v, err := d.Version()
	if err != nil {
		t.Error(err)
	} else if v != 1 {
		t.Errorf("expected version %d to equal %d", v, 1)
	}

	pipe = pipep.New()
	go d.Migrate(files[1], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// check version
	v, err = d.Version()
	if err != nil {
		t.Error(err)
	} else if v != 0 {
		t.Errorf("expected version %d to equal %d", v, 0)
	}

	pipe = pipep.New()
	go d.Migrate(files[2], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) == 0 {
		t.Error("Expected test case to fail")
	}

	// check version
	v, err = d.Version()
	if err != nil {
		t.Error(err)
	} else if v != 0 {
		t.Errorf("expected version %d to equal %d", v, 0)
	}

	// run 0 again
	pipe = pipep.New()
	go d.Migrate(files[0], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// check version
	v, err = d.Version()
	if err != nil {
		t.Error(err)
	} else if v != 1 {
		t.Errorf("expected version %d to equal %d", v, 1)
	}

	// run 3
	pipe = pipep.New()
	go d.Migrate(files[3], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// check version
	v, err = d.Version()
	if err != nil {
		t.Error(err)
	} else if v != 2 {
		t.Errorf("expected version %d to equal %d", v, 2)
	}

	// run 4
	pipe = pipep.New()
	go d.Migrate(files[4], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// check version
	v, err = d.Version()
	if err != nil {
		t.Error(err)
	} else if v != 1 {
		t.Errorf("expected version %d to equal %d", v, 1)
	}

	// run 1 again
	pipe = pipep.New()
	go d.Migrate(files[1], pipe)
	errs = pipep.ReadErrors(pipe)
	if len(errs) > 0 {
		t.Fatal(errs)
	}

	// check version
	v, err = d.Version()
	if err != nil {
		t.Error(err)
	} else if v != 0 {
		t.Errorf("expected version %d to equal %d", v, 0)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}
