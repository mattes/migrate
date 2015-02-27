package migrate

import (
	"io/ioutil"
	"testing"

	"github.com/fedyakin/migrate/driver"
)

// Add Driver URLs here to test basic Up, Down, .. functions.
var driverUrls = []string{
	"postgres://localhost/migratetest?sslmode=disable",
}

func TestCreate(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		if _, err := Create(driverUrl, tmpdir, "test_migration", driver.TxnPerFile); err != nil {
			t.Fatal(err)
		}
		if _, err := Create(driverUrl, tmpdir, "another migration", driver.TxnPerFile); err != nil {
			t.Fatal(err)
		}

		files, err := ioutil.ReadDir(tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 4 {
			t.Fatal("Expected 2 new files, got", len(files))
		}
		expectFiles := []string{
			"0001_test_migration.up.sql", "0001_test_migration.down.sql",
			"0002_another_migration.up.sql", "0002_another_migration.down.sql",
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
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1", driver.TxnPerFile)
		Create(driverUrl, tmpdir, "migration2", driver.TxnPerFile)

		errs, ok := ResetSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
	}
}

func TestDown(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1", driver.TxnPerFile)
		Create(driverUrl, tmpdir, "migration2", driver.TxnPerFile)

		errs, ok := ResetSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = DownSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}
	}
}

func TestUp(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1", driver.TxnPerFile)
		Create(driverUrl, tmpdir, "migration2", driver.TxnPerFile)

		errs, ok := DownSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = UpSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
	}
}

func TestRedo(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1", driver.TxnPerFile)
		Create(driverUrl, tmpdir, "migration2", driver.TxnPerFile)

		errs, ok := ResetSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = RedoSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}
	}
}

func TestMigrate(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1", driver.TxnPerFile)
		Create(driverUrl, tmpdir, "migration2", driver.TxnPerFile)

		errs, ok := ResetSync(driverUrl, tmpdir, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatalf("Expected version 2, got %v", version)
		}

		errs, ok = MigrateSync(driverUrl, tmpdir, -2, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 0 {
			t.Fatalf("Expected version 0, got %v", version)
		}

		errs, ok = MigrateSync(driverUrl, tmpdir, +1, driver.TxnPerFile)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir, driver.TxnPerFile)
		if err != nil {
			t.Fatal(err)
		}
		if version != 1 {
			t.Fatalf("Expected version 1, got %v", version)
		}
	}
}
