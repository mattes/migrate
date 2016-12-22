// Package bash implements the Driver interface.
package bash

import (
	"github.com/mattes/migrate/driver"
	"github.com/mattes/migrate/file"
)

type Driver struct {
}

func (driver *Driver) Initialize(url string) error {
	return nil
}

func (driver *Driver) Close() error {
	return nil
}

func (driver *Driver) FilenameExtension() string {
	return "sh"
}

func (driver *Driver) Migrate(f file.File, pipe chan interface{}) {
	defer close(pipe)
	pipe <- f
	return
}

func (driver *Driver) Version() (file.Version, error) {
	return file.Version(0), nil
}

func (driver *Driver) Versions() (file.Versions, error) {
	return file.Versions{0}, nil
}

func init() {
	driver.RegisterDriver("bash", &Driver{})
}
