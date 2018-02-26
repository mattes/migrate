package git

import (
	"errors"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
	nurl "net/url"
	"os"

	"github.com/mattes/migrate/source"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	githttp "gopkg.in/src-d/go-git.v4/plumbing/transport/http"
	gitssh "gopkg.in/src-d/go-git.v4/plumbing/transport/ssh"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"strings"
)

const (
	paramSSHKeyPath = "ssh-key-path"
	envHome         = "HOME"
	schemeSSH       = "ssh"
	schemeHTTPS     = "https"
)

var (
	errNoValidCerts       = errors.New("no valid ssh certificate could be loaded")
	errInvalidURL         = errors.New("invalid url")
	errSchemeNotSupported = errors.New("scheme not supported")
	errUserMissing        = errors.New("user is required")
	errOwnerMissing       = errors.New("owner is required")
	errRepoMissing        = errors.New("repo is required")
	errHostMissing        = errors.New("host is required")
)

func init() {
	source.Register("ssh", &Git{})
	source.Register("https", &Git{})
}

// Git is the underlying struct used by the source driver.
type Git struct {
	path       string
	repo       *git.Repository
	migrations *source.Migrations
}

// Config can be passed in when using with instance.
type Config struct {
	Path string
}

// RepoMeta is the struct which gets populated from the parsed in git url.
type RepoMeta struct {
	Scheme   string
	Owner    string
	User     string
	Password string
	Repo     string
	Path     string
	Host     string
	Certs    []string
}

// FullRepoName will return the full "owner/repo" name based on the values stored in the repo meta struct. If one
// of the dependent values are missing an error is returned.
func (m *RepoMeta) FullRepoName() (string, error) {

	if m.Owner == "" {
		return "", errOwnerMissing
	}

	if m.Repo == "" {
		return "", errRepoMissing
	}

	return fmt.Sprintf("%v/%v", m.Owner, m.Repo), nil
}

// URL will return the url based on the the repo metas scheme value. If the scheme is not supported an error will be
// returned.
func (m *RepoMeta) URL() (string, error) {

	fullRepoName, err := m.FullRepoName()
	if err != nil {
		return "", err
	}

	if m.Host == "" {
		return "", errHostMissing
	}

	switch m.Scheme {
	case schemeSSH:
		return fmt.Sprintf("%v@%v:%v", m.User, m.Host, fullRepoName), nil

	case schemeHTTPS:
		return fmt.Sprintf("https://%v/%v", m.Host, fullRepoName), nil
	}

	return "", errSchemeNotSupported
}

// Validate is used to validate the repo meta struct.
func (m *RepoMeta) Validate() error {

	switch m.Scheme {
	case schemeSSH:
		if m.User == "" {
			return errUserMissing
		}

	case schemeHTTPS:

	default:
		return errSchemeNotSupported
	}

	if m.Host == "" {
		return errHostMissing
	}

	if m.Owner == "" {
		return errOwnerMissing
	}

	if m.Repo == "" {
		return errRepoMissing
	}

	return nil
}

// Parse is used to read in the url and stores the information in the url to a repo meta struct.
func Parse(url string) (*RepoMeta, error) {

	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(u.Path, "/")
	if len(parts) < 3 {
		return nil, errInvalidURL
	}

	m := RepoMeta{
		Scheme: u.Scheme,
		Owner:  parts[1],
		Repo:   parts[2],
		Path:   strings.Join(parts[3:], "/"),
		Host:   u.Host,
	}

	m.Certs = u.Query()[paramSSHKeyPath]
	m.Certs = append(
		m.Certs,
		fmt.Sprintf("%v/.ssh/id_rsa", os.Getenv(envHome)),
		fmt.Sprintf("%v/.ssh/id_dsa", os.Getenv(envHome)),
	)

	if u.User != nil {
		m.User = u.User.Username()
		m.Password, _ = u.User.Password()
	}

	err = m.Validate()
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// NewRepository is used to return a new get repository, repo path or an error if any errors are encountered whist
// creating the git repository client.
func NewRepository(url string) (*git.Repository, string, error) {

	m, err := Parse(url)
	if err != nil {
		return nil, "", err
	}

	var gitAuth transport.AuthMethod
	var gitURL string

	switch m.Scheme {
	case schemeSSH:

		gitURL, err = m.URL()

		for _, cert := range m.Certs {

			b, err := ioutil.ReadFile(cert)
			if err != nil {
				continue
			}

			signer, err := ssh.ParsePrivateKey(b)
			if err != nil {
				continue
			}

			gitAuth = &gitssh.PublicKeys{
				User:   m.User,
				Signer: signer,
			}

			break
		}

	case schemeHTTPS:

		gitURL, err = m.URL()
		if m.User != "" {
			gitAuth = &githttp.BasicAuth{Username: m.User, Password: m.Password}
		}
	}

	fs := memfs.New()

	r, err := git.Clone(memory.NewStorage(), fs, &git.CloneOptions{
		URL:  gitURL,
		Auth: gitAuth,
	})

	return r, m.Path, nil
}

// Open is used to return a source driver based on the url string passed.
func (g *Git) Open(url string) (source.Driver, error) {

	repo, path, err := NewRepository(url)
	if err != nil {
		return nil, err
	}

	gn := &Git{
		repo:       repo,
		path:       path,
		migrations: source.NewMigrations(),
	}

	if err := gn.readDirectory(); err != nil {
		return nil, err
	}

	return gn, nil
}

// WithInstance is used to pass in an existing git repo instance. A config must also be passed to include any of the
// required configs.
func WithInstance(repo *git.Repository, config *Config) (source.Driver, error) {

	gn := &Git{
		repo:       repo,
		path:       config.Path,
		migrations: source.NewMigrations(),
	}
	if err := gn.readDirectory(); err != nil {
		return nil, err
	}
	return gn, nil
}

func (g *Git) readDirectory() error {

	w, err := g.repo.Worktree()
	if err != nil {
		return err
	}

	w.Checkout(&git.CheckoutOptions{
		Hash: plumbing.NewHash(""),
	})

	fullPath := fmt.Sprintf("/%s", g.path)

	_, err = w.Filesystem.Stat(fullPath)
	if err != nil {
		return err
	}

	files, err := w.Filesystem.ReadDir(fullPath)
	if err != nil {
		return err
	}

	for _, fi := range files {

		if fi.IsDir() {
			continue
		}

		m, err := source.DefaultParse(fi.Name())
		if err != nil {
			continue
		}

		if !g.migrations.Append(m) {
			return fmt.Errorf("unable to parse file %v", fi.Name())
		}
	}

	return nil
}

// Close is used to close the connection to the git repository.
func (g *Git) Close() error {
	return nil
}

// First is used to return the first migration version encountered.
func (g *Git) First() (version uint, er error) {
	if v, ok := g.migrations.First(); !ok {
		return 0, &os.PathError{"first", g.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

// Prev is used to return the previous migration version.
func (g *Git) Prev(version uint) (prevVersion uint, err error) {
	if v, ok := g.migrations.Prev(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("prev for version %v", version), g.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

// Next is used to return the next migration version.
func (g *Git) Next(version uint) (nextVersion uint, err error) {
	if v, ok := g.migrations.Next(version); !ok {
		return 0, &os.PathError{fmt.Sprintf("next for version %v", version), g.path, os.ErrNotExist}
	} else {
		return v, nil
	}
}

// ReadUp reads in the next migration.
func (g *Git) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {

	if m, ok := g.migrations.Up(version); ok {
		w, err := g.repo.Worktree()
		if err != nil {
			return nil, "", err
		}

		f, err := w.Filesystem.Open(fmt.Sprintf("/%s/%s", g.path, m.Raw))
		if err != nil {
			return nil, "", err
		}

		return f, m.Identifier, nil
	}
	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), g.path, os.ErrNotExist}
}

// ReadDown reads in the previous migration.
func (g *Git) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	if m, ok := g.migrations.Down(version); ok {
		w, err := g.repo.Worktree()
		if err != nil {
			return nil, "", err
		}

		f, err := w.Filesystem.Open(fmt.Sprintf("/%s/%s", g.path, m.Raw))
		if err != nil {
			return nil, "", err
		}

		return f, m.Identifier, nil
	}
	return nil, "", &os.PathError{fmt.Sprintf("read version %v", version), g.path, os.ErrNotExist}
}
