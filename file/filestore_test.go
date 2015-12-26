package file

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	. "github.com/onsi/gomega"
)

func TestFSStore(t *testing.T) {
	RegisterTestingT(t)

	var files []string
	var bs []byte

	tmpdir, err := ioutil.TempDir("/tmp", "TestLookForMigrationFilesInSearchPath")
	Ω(err).ShouldNot(HaveOccurred())
	defer os.RemoveAll(tmpdir)

	err = ioutil.WriteFile(path.Join(tmpdir, "a"), []byte("a"), 0755)
	Ω(err).ShouldNot(HaveOccurred())

	err = os.Mkdir(path.Join(tmpdir, "x"), 0755)
	Ω(err).ShouldNot(HaveOccurred())
	err = ioutil.WriteFile(path.Join(tmpdir, "x/b"), []byte("b"), 0755)
	Ω(err).ShouldNot(HaveOccurred())
	err = ioutil.WriteFile(path.Join(tmpdir, "x/c"), nil, 0755)
	Ω(err).ShouldNot(HaveOccurred())

	err = os.Mkdir(path.Join(tmpdir, "y"), 0755)
	Ω(err).ShouldNot(HaveOccurred())

	files, err = FSStore{}.ReadDir(tmpdir)
	Ω(err).ShouldNot(HaveOccurred())
	Ω(files).Should(ConsistOf("a", "x", "y"))

	files, err = FSStore{}.ReadDir(path.Join(tmpdir, "x"))
	Ω(err).ShouldNot(HaveOccurred())
	Ω(files).Should(ConsistOf("b", "c"))

	files, err = FSStore{}.ReadDir(path.Join(tmpdir, "y"))
	Ω(err).ShouldNot(HaveOccurred())
	Ω(files).Should(BeEmpty())

	files, err = FSStore{}.ReadDir(path.Join(tmpdir, "XXX"))
	Ω(err).Should(HaveOccurred())
	Ω(files).Should(BeNil())

	bs, err = FSStore{}.ReadFile(&File{
		Path:     tmpdir,
		FileName: "a",
	})
	Ω(err).ShouldNot(HaveOccurred())
	Ω(bs).Should(Equal([]byte("a")))

	bs, err = FSStore{}.ReadFile(&File{
		Path:     path.Join(tmpdir, "x"),
		FileName: "b",
	})
	Ω(err).ShouldNot(HaveOccurred())
	Ω(bs).Should(Equal([]byte("b")))

	bs, err = FSStore{}.ReadFile(&File{
		Path:     path.Join(tmpdir, "x"),
		FileName: "AAA",
	})
	Ω(err).Should(HaveOccurred())
	Ω(bs).Should(BeNil())

	bs, err = FSStore{}.ReadFile(&File{
		Path:     path.Join(tmpdir, "ZZZ"),
		FileName: "b",
	})
	Ω(err).Should(HaveOccurred())
	Ω(bs).Should(BeNil())
}

func TestAssetStore(t *testing.T) {
	RegisterTestingT(t)

	var files []string
	var bs []byte
	var err error

	store := AssetStore{
		Asset: func(path string) ([]byte, error) {
			switch path {
			case "a":
				return []byte("a"), nil
			case "x/b":
				return []byte("b"), nil
			case "x/c":
				return []byte{}, nil
			default:
				return nil, fmt.Errorf("unknown file: %s", path)
			}
		},
		AssetDir: func(path string) ([]string, error) {
			switch path {
			case "":
				return []string{"a", "x", "y"}, nil
			case "x":
				return []string{"b", "c"}, nil
			case "y":
				return []string{}, nil
			default:
				return nil, fmt.Errorf("unknown dir: %s", path)
			}
		},
	}

	files, err = store.ReadDir("")
	Ω(err).ShouldNot(HaveOccurred())
	Ω(files).Should(ConsistOf("a", "x", "y"))

	files, err = store.ReadDir("x")
	Ω(err).ShouldNot(HaveOccurred())
	Ω(files).Should(ConsistOf("b", "c"))

	files, err = store.ReadDir("y")
	Ω(err).ShouldNot(HaveOccurred())
	Ω(files).Should(BeEmpty())

	files, err = store.ReadDir("XXX")
	Ω(err).Should(HaveOccurred())
	Ω(files).Should(BeNil())

	bs, err = store.ReadFile(&File{
		FileName: "a",
	})
	Ω(err).ShouldNot(HaveOccurred())
	Ω(bs).Should(Equal([]byte("a")))

	bs, err = store.ReadFile(&File{
		Path:     "x",
		FileName: "b",
	})
	Ω(err).ShouldNot(HaveOccurred())
	Ω(bs).Should(Equal([]byte("b")))

	bs, err = store.ReadFile(&File{
		Path:     "x",
		FileName: "AAA",
	})
	Ω(err).Should(HaveOccurred())
	Ω(bs).Should(BeNil())

	bs, err = store.ReadFile(&File{
		Path:     "ZZZ",
		FileName: "b",
	})
	Ω(err).Should(HaveOccurred())
	Ω(bs).Should(BeNil())
}
