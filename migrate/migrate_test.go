package migrate

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	// Ensure imports for each driver we wish to test

	_ "github.com/mattes/migrate/driver/postgres"
	_ "github.com/mattes/migrate/driver/sqlite3"
)

// Add Driver URLs here to test basic Up, Down, .. functions.
var driverUrls = []string{
	"postgres://postgres@" + os.Getenv("POSTGRES_PORT_5432_TCP_ADDR") + ":" + os.Getenv("POSTGRES_PORT_5432_TCP_PORT") + "/template1?sslmode=disable",
}

func TestCreate(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		file1, err := Create(driverUrl, tmpdir, "test_migration")
		if err != nil {
			t.Fatal(err)
		}
		file2, err := Create(driverUrl, tmpdir, "another migration")
		if err != nil {
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
			file1.UpFile.FileName, file1.DownFile.FileName,
			file2.UpFile.FileName, file2.DownFile.FileName,
		}
		for _, expectFile := range expectFiles {
			filepath := path.Join(tmpdir, expectFile)
			if _, err := os.Stat(filepath); os.IsNotExist(err) {
				t.Errorf("Can't find migration file: %s", filepath)
			}
		}

		if file1.Version == file2.Version {
			t.Errorf("files can't same version: %d", file1.Version)
		}
	}
}

func TestReset(t *testing.T) {
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1")
		file, _ := Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != file.Version {
			t.Fatalf("Expected version %d, got %v", file.Version, version)
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

		Create(driverUrl, tmpdir, "migration1")
		file, _ := Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != file.Version {
			t.Fatalf("Expected version %d, got %v", file.Version, version)
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
	for _, driverUrl := range driverUrls {
		t.Logf("Test driver: %s", driverUrl)
		tmpdir, err := ioutil.TempDir("/tmp", "migrate-test")
		if err != nil {
			t.Fatal(err)
		}

		Create(driverUrl, tmpdir, "migration1")
		file, _ := Create(driverUrl, tmpdir, "migration2")

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
		if version != file.Version {
			t.Fatalf("Expected version %d, got %v", file.Version, version)
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

		Create(driverUrl, tmpdir, "migration1")
		file, _ := Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != file.Version {
			t.Fatalf("Expected version %d, got %v", file.Version, version)
		}

		errs, ok = RedoSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err = Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != file.Version {
			t.Fatalf("Expected version %d, got %v", file.Version, version)
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

		file1, _ := Create(driverUrl, tmpdir, "migration1")
		file2, _ := Create(driverUrl, tmpdir, "migration2")

		errs, ok := ResetSync(driverUrl, tmpdir)
		if !ok {
			t.Fatal(errs)
		}
		version, err := Version(driverUrl, tmpdir)
		if err != nil {
			t.Fatal(err)
		}
		if version != file2.Version {
			t.Fatalf("Expected version %d, got %v", file2.Version, version)
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
		if version != file1.Version {
			t.Fatalf("Expected version %d, got %v", file1.Version, version)
		}
	}
}
