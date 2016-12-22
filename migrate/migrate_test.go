package migrate

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
	// Ensure imports for each driver we wish to test

	"github.com/jmhodges/clock"
	_ "github.com/mattes/migrate/driver/postgres"
	_ "github.com/mattes/migrate/driver/sqlite3"
)

// Add Driver URLs here to test basic Up, Down, .. functions.
var driverUrls = []string{
	"postgres://postgres@" + os.Getenv("POSTGRES_PORT_5432_TCP_ADDR") + ":" + os.Getenv("POSTGRES_PORT_5432_TCP_PORT") + "/template1?sslmode=disable",
}

func TestCreate(t *testing.T) {
	clk := clock.NewFake()
	globalClock = clk
	defer func() {
		globalClock = clock.New()
	}()
	for _, driverUrl := range driverUrls {
		clk.Set(time.Unix(0, 0))
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		clk.Add(1 * time.Second)
		if _, err := Create(driverUrl, tmpdir, "test_migration"); err != nil {
			t.Fatal(err)
		}
		clk.Add(1 * time.Second)
		if _, err := Create(driverUrl, tmpdir, "another migration"); err != nil {
			t.Fatal(err)
		}

		files, err := ioutil.ReadDir(tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 4 {
			t.Fatal("Expected 4 new files, got", len(files))
		}
		expectFiles := []string{
			"19700101000001_test_migration.up.sql",
			"19700101000001_test_migration.down.sql",
			"19700101000002_another_migration.up.sql",
			"19700101000002_another_migration.down.sql",
		}
		foundCounter := 0
		for _, expectFile := range expectFiles {
			for _, file := range files {
				if expectFile == file.Name() {
					foundCounter += 1
					break
				}
			}
		}
		if foundCounter != len(expectFiles) {
			t.Error("not all expected files have been found")
		}
	}
}

func TestReset(t *testing.T) {
	clk := clock.NewFake()
	globalClock = clk
	defer func() {
		globalClock = clock.New()
	}()
	for _, driverUrl := range driverUrls {
		clk.Set(time.Unix(0, 0))
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration1")
		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 19700101000002 {
			t.Fatalf("Expected version 19700101000002, got %v", version)
		}
	}
}

func TestDown(t *testing.T) {
	clk := clock.NewFake()
	globalClock = clk
	defer func() {
		globalClock = clock.New()
	}()
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration1")
		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 19700101000002 {
			t.Fatalf("Expected version 19700101000002, got %v", version)
		}

		errs, ok = DownSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}
	}
}

func TestUp(t *testing.T) {
	clk := clock.NewFake()
	globalClock = clk
	defer func() {
		globalClock = clock.New()
	}()
	for _, driverUrl := range driverUrls {
		clk.Set(time.Unix(0, 0))
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration1")
		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := DownSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = UpSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 19700101000002 {
			t.Fatalf("Expected version 19700101000002, got %v", version)
		}
	}
}

func TestRedo(t *testing.T) {
	clk := clock.NewFake()
	globalClock = clk
	defer func() {
		globalClock = clock.New()
	}()
	for _, driverUrl := range driverUrls {
		clk.Set(time.Unix(0, 0))
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration1")
		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 19700101000002 {
			t.Fatalf("Expected version 19700101000002, got %v", version)
		}

		errs, ok = RedoSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 19700101000002 {
			t.Fatalf("Expected version 19700101000002, got %v", version)
		}
	}
}

func TestMigrate(t *testing.T) {
	clk := clock.NewFake()
	globalClock = clk
	defer func() {
		globalClock = clock.New()
	}()
	for _, driverUrl := range driverUrls {
		clk.Set(time.Unix(0, 0))
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration1")
		clk.Add(1 * time.Second)
		Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 19700101000002 {
			t.Fatalf("Expected version 19700101000002, got %v", version)
		}

		errs, ok = MigrateSync(driverUrl, tmpdir, -2)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = MigrateSync(driverUrl, tmpdir, +1)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != 19700101000001 {
			t.Fatalf("Expected version 19700101000001, got %v", version)
		}
	}
}
