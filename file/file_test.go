package file

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/mattes/migrate/migrate/direction"
)

func TestParseFilenameSchema(t *testing.T) {
	var tests = []struct {
		filename          string
		filenameExtension string
		expectVersion     Version
		expectName        string
		expectDirection   direction.Direction
		expectErr         bool
	}{
		{"001_test_file.up.sql", "sql", 1, "test_file", direction.Up, false},
		{"001_test_file.down.sql", "sql", 1, "test_file", direction.Down, false},
		{"10034_test_file.down.sql", "sql", 10034, "test_file", direction.Down, false},
		{"-1_test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"test_file.down.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file.down", "sql", 0, "", direction.Up, true},
		{"100_test_file.sql", "sql", 0, "", direction.Up, true},
		{"100_test_file", "sql", 0, "", direction.Up, true},
		{"test_file", "sql", 0, "", direction.Up, true},
		{"100", "sql", 0, "", direction.Up, true},
		{".sql", "sql", 0, "", direction.Up, true},
		{"up.sql", "sql", 0, "", direction.Up, true},
		{"down.sql", "sql", 0, "", direction.Up, true},
	}

	for _, test := range tests {
		version, name, migrate, err := parseFilenameSchema(test.filename, FilenameRegex(test.filenameExtension))
		if test.expectErr && err == nil {
			t.Fatal("Expected error, but got none.", test)
		}
		if !test.expectErr && err != nil {
			t.Fatal("Did not expect error, but got one:", err, test)
		}
		if err == nil {
			if version != test.expectVersion {
				t.Error("Wrong version number", test)
			}
			if name != test.expectName {
				t.Error("wrong name", test)
			}
			if migrate != test.expectDirection {
				t.Error("wrong migrate", test)
			}
		}
	}
}

func TestFiles(t *testing.T) {
	tmpdir, err := ioutil.TempDir("/tmp", "TestLookForMigrationFilesInSearchPath")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	if err := ioutil.WriteFile(path.Join(tmpdir, "nonsense.txt"), nil, 0755); err != nil {
		t.Fatal("Unable to write files in tmpdir", err)
	}
	ioutil.WriteFile(path.Join(tmpdir, "002_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, "002_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "001_migrationfile.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, "001_migrationfile.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "101_create_table.up.sql"), nil, 0755)
	ioutil.WriteFile(path.Join(tmpdir, "101_drop_tables.down.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "301_migrationfile.up.sql"), nil, 0755)

	ioutil.WriteFile(path.Join(tmpdir, "401_migrationfile.down.sql"), []byte("test"), 0755)

	files, err := ReadMigrationFiles(tmpdir, FilenameRegex("sql"))
	if err != nil {
		t.Fatal(err)
	}

	if len(files) == 0 {
		t.Fatal("No files returned.")
	}

	if len(files) != 5 {
		t.Fatal("Wrong number of files returned.")
	}

	// test sort order
	if files[0].Version != 1 || files[1].Version != 2 || files[2].Version != 101 || files[3].Version != 301 || files[4].Version != 401 {
		t.Error("Sort order is incorrect")
		t.Error(files)
	}

	// test UpFile and DownFile
	if files[0].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[0].Version)
	}
	if files[0].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[0].Version)
	}

	if files[1].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[1].Version)
	}
	if files[1].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[1].Version)
	}

	if files[2].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[2].Version)
	}
	if files[2].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[2].Version)
	}

	if files[3].UpFile == nil {
		t.Fatalf("Missing up file for version %v", files[3].Version)
	}
	if files[3].DownFile != nil {
		t.Fatalf("There should not be a down file for version %v", files[3].Version)
	}

	if files[4].UpFile != nil {
		t.Fatalf("There should not be a up file for version %v", files[4].Version)
	}
	if files[4].DownFile == nil {
		t.Fatalf("Missing down file for version %v", files[4].Version)
	}

	// test read
	if err := files[4].DownFile.ReadContent(); err != nil {
		t.Error("Unable to read file", err)
	}
	if files[4].DownFile.Content == nil {
		t.Fatal("Read content is nil")
	}
	if string(files[4].DownFile.Content) != "test" {
		t.Fatal("Read content is wrong")
	}

	// test names
	if files[0].UpFile.Name != "migrationfile" {
		t.Error("file name is not correct", files[0].UpFile.Name)
	}
	if files[0].UpFile.FileName != "001_migrationfile.up.sql" {
		t.Error("file name is not correct", files[0].UpFile.FileName)
	}

	// test file.Relative()
	// there should be the following versions:
	// 1(up&down), 2(up&down), 101(up&down), 301(up), 401(down)
	var tests = []struct {
		appliedVersions Versions
		relative        int
		expectRange     Versions
	}{
		{Versions{}, 2, Versions{1, 2}},
		{Versions{1}, 4, Versions{2, 101, 301}},
		{Versions{1}, 0, nil},
		{Versions{}, 1, Versions{1}},
		{Versions{}, 0, nil},
		{Versions{1, 2, 101}, -2, Versions{101, 2}},
		{Versions{1, 2, 101, 301, 401}, -1, Versions{401}},
	}

	for _, test := range tests {
		rangeFiles, err := files.Relative(test.relative, test.appliedVersions)
		if err != nil {
			t.Error("Unable to fetch range:", err)
		}
		if len(rangeFiles) != len(test.expectRange) {
			t.Fatalf("file.Relative(): expected %v files, got %v. For test %v.", len(test.expectRange), len(rangeFiles), test.expectRange)
		}

		for i, version := range test.expectRange {
			if rangeFiles[i].Version != version {
				t.Logf("rangeFiles: %v\n", rangeFiles)
				t.Fatal("file.Relative(): returned files dont match expectations", test.expectRange)
			}
		}
	}
}

func TestDuplicateFiles(t *testing.T) {
	dups := []string{
		"001_migration.up.sql",
		"001_duplicate.up.sql",
	}

	root, cleanFn, err := makeFiles("TestDuplicateFiles", dups...)
	defer cleanFn()

	if err != nil {
		t.Fatal(err)
	}

	_, err = ReadMigrationFiles(root, FilenameRegex("sql"))
	if err == nil {
		t.Fatal("Expected duplicate migration file error")
	}
}

// makeFiles takes an identifier, and a list of file names and uses them to create a temporary
// directory populated with files named with the names passed in.  makeFiles returns the root
// directory name, and a func suitable for a defer cleanup to remove the temporary files after
// the calling function exits.
func makeFiles(testname string, names ...string) (root string, cleanup func(), err error) {
	cleanup = func() {}
	root, err = ioutil.TempDir("/tmp", testname)
	if err != nil {
		return
	}
	cleanup = func() { os.RemoveAll(root) }
	if err = ioutil.WriteFile(path.Join(root, "nonsense.txt"), nil, 0755); err != nil {
		return
	}

	for _, name := range names {
		if err = ioutil.WriteFile(path.Join(root, name), nil, 0755); err != nil {
			return
		}
	}
	return
}
