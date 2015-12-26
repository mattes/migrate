package file

import (
	"io/ioutil"
	"path"
)

// FileStore interface is an abstraction layer with minimal functionality
// to get a list of migration files and read their contents.
type FileStore interface {
	// Read contents of a given file
	ReadFile(*File) ([]byte, error)
	// List filenames of a given directory (including subdirectories)
	ReadDir(string) ([]string, error)
}

// FSStore is a regular file system store
type FSStore struct{}

// Read contents of a file
func (s FSStore) ReadFile(f *File) ([]byte, error) {
	return ioutil.ReadFile(path.Join(f.Path, f.FileName))
}

// List file in a given dir
func (s FSStore) ReadDir(dirname string) ([]string, error) {
	if fs, err := ioutil.ReadDir(dirname); err != nil {
		return nil, err
	} else {
		res := make([]string, len(fs))
		for i := range fs {
			res[i] = fs[i].Name()
		}
		return res, nil
	}
}

// AssetStore is a bindatata asset store
type AssetStore struct {
	// Asset should return content of file in path if exists
	Asset func(path string) ([]byte, error)
	// AssetDir should return list of files in the path
	AssetDir func(path string) ([]string, error)
}

// Read contents of a file
func (s AssetStore) ReadFile(f *File) ([]byte, error) {
	return s.Asset(path.Join(f.Path, f.FileName))
}

// List file in a given dir
func (s AssetStore) ReadDir(dirname string) ([]string, error) {
	return s.AssetDir(dirname)
}
