// Package migrate is imported by other Go code.
// It is the entry point to all migration functions.
package migrate

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

// Up applies all available migrations
func Up(pipe chan interface{}, url, migrationsPath string) {
	d, files, versions, err := initDriverAndReadMigrationFilesAndGetVersions(url, migrationsPath)
	defer func() {
		if err != nil {
			pipe <- err

		}
		if err = d.Close(); err != nil {
			pipe <- err
		}
		go pipep.Close(pipe, nil)
	}()

	applyMigrationFiles, err := files.Pending(versions)
	if err != nil {
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
	}
}

// UpSync is synchronous version of Up
func UpSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Up(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Down rolls back all migrations
func Down(pipe chan interface{}, url, migrationsPath string) {
	d, files, versions, err := initDriverAndReadMigrationFilesAndGetVersions(url, migrationsPath)
	defer func() {
		if err != nil {
			pipe <- err

		}
		if err = d.Close(); err != nil {
			pipe <- err
		}
		go pipep.Close(pipe, nil)
	}()
	if err != nil {
		return
	}

	applyMigrationFiles, err := files.Applied(versions)
	if err != nil {
		return
	}

	if len(applyMigrationFiles) > 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
	}
}

// DownSync is synchronous version of Down
func DownSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Down(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(pipe chan interface{}, url, migrationsPath string) {
	pipe1 := pipep.New()
	go Migrate(pipe1, url, migrationsPath, -1)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	}
	go Migrate(pipe, url, migrationsPath, +1)
}

// RedoSync is synchronous version of Redo
func RedoSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Redo(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Reset runs the down and up migration function
func Reset(pipe chan interface{}, url, migrationsPath string) {
	pipe1 := pipep.New()
	go Down(pipe1, url, migrationsPath)
	if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
		go pipep.Close(pipe, nil)
		return
	}
	go Up(pipe, url, migrationsPath)
}

// ResetSync is synchronous version of Reset
func ResetSync(url, migrationsPath string) (err []error, ok bool) {
	pipe := pipep.New()
	go Reset(pipe, url, migrationsPath)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Migrate applies relative +n/-n migrations
func Migrate(pipe chan interface{}, url, migrationsPath string, relativeN int) {
	d, files, versions, err := initDriverAndReadMigrationFilesAndGetVersions(url, migrationsPath)
	defer func() {
		if err != nil {
			pipe <- err

		}
		if err = d.Close(); err != nil {
			pipe <- err
		}
		go pipep.Close(pipe, nil)
	}()
	if err != nil {
		return
	}

	applyMigrationFiles, err := files.Relative(relativeN, versions)
	if err != nil {
		return
	}

	if len(applyMigrationFiles) > 0 && relativeN != 0 {
		for _, f := range applyMigrationFiles {
			pipe1 := pipep.New()
			go d.Migrate(f, pipe1)
			if ok := pipep.WaitAndRedirect(pipe1, pipe, handleInterrupts()); !ok {
				break
			}
		}
	}
}

// MigrateSync is synchronous version of Migrate
func MigrateSync(url, migrationsPath string, relativeN int) (err []error, ok bool) {
	pipe := pipep.New()
	go Migrate(pipe, url, migrationsPath, relativeN)
	err = pipep.ReadErrors(pipe)
	return err, len(err) == 0
}

// Version returns the current migration version
func Version(url, migrationsPath string) (version file.Version, err error) {
	d, err := driver.New(url)
	if err != nil {
		return 0, err
	}
	return d.Version()
}

// Version returns applied versions
func Versions(url, migrationsPath string) (versions file.Versions, err error) {
	d, err := driver.New(url)
	if err != nil {
		return file.Versions{}, err
	}
	return d.Versions()
}

// Create creates new migration files on disk
func Create(url, migrationsPath, name string) (*file.MigrationFile, error) {
	d, files, _, err := initDriverAndReadMigrationFilesAndGetVersions(url, migrationsPath)
	if err != nil {
		return nil, err
	}

	versionStr := time.Now().UTC().Format("20060102150405")
	v, _ := strconv.ParseUint(versionStr, 10, 64)
	version := file.Version(v)

	filenamef := "%d_%s.%s.%s"
	name = strings.Replace(name, " ", "_", -1)

	// if latest version has the same timestamp, increment version
	if len(files) > 0 {
		latest := files[len(files)-1].Version
		if latest >= version {
			version = latest + 1
		}
	}

	mfile := &file.MigrationFile{
		Version: version,
		UpFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, version, name, "up", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Up,
		},
		DownFile: &file.File{
			Path:      migrationsPath,
			FileName:  fmt.Sprintf(filenamef, version, name, "down", d.FilenameExtension()),
			Name:      name,
			Content:   []byte(""),
			Direction: direction.Down,
		},
	}

	if err := ioutil.WriteFile(path.Join(mfile.UpFile.Path, mfile.UpFile.FileName), mfile.UpFile.Content, 0644); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(path.Join(mfile.DownFile.Path, mfile.DownFile.FileName), mfile.DownFile.Content, 0644); err != nil {
		return nil, err
	}

	return mfile, nil
}

// initDriverAndReadMigrationFilesAndGetVersionsAndGetVersion is a small helper
// function that is common to most of the migration funcs
func initDriverAndReadMigrationFilesAndGetVersions(url, migrationsPath string) (driver.Driver, file.MigrationFiles, file.Versions, error) {
	d, err := driver.New(url)
	if err != nil {
		return nil, nil, file.Versions{}, err
	}
	files, err := file.ReadMigrationFiles(migrationsPath, file.FilenameRegex(d.FilenameExtension()))
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, file.Versions{}, err
	}

	versions, err := d.Versions()
	if err != nil {
		d.Close() // TODO what happens with errors from this func?
		return nil, nil, file.Versions{}, err

	}

	return d, files, versions, nil
}

// NewPipe is a convenience function for pipe.New().
// This is helpful if the user just wants to import this package and nothing else.
func NewPipe() chan interface{} {
	return pipep.New()
}

// interrupts is an internal variable that holds the state of
// interrupt handling
var interrupts = true

// Graceful enables interrupts checking. Once the first ^C is received
// it will finish the currently running migration and abort execution
// of the next migration. If ^C is received twice, it will stop
// execution immediately.
func Graceful() {
	interrupts = true
}

// NonGraceful disables interrupts checking. The first received ^C will
// stop execution immediately.
func NonGraceful() {
	interrupts = false
}

// interrupts returns a signal channel if interrupts checking is
// enabled. nil otherwise.
func handleInterrupts() chan os.Signal {
	if interrupts {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		return c
	}
	return nil
}
