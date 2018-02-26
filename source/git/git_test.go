package git

import (
	"fmt"
	st "github.com/mattes/migrate/source/testing"
	"gopkg.in/src-d/go-billy.v3/memfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"os"
	"regexp"
	"testing"
)

const (
	urlHTTP                     = "https://github.com/mattes/migrate_test.git/test"
	urlSSH                      = "ssh://git@github.com/mattes/migrate_test.git/test"
	urlSSHWithoutPath           = "ssh://git@github.com/mattes/migrate_test.git"
	urlSSHWithBadPath           = "ssh://git@github.com/mattes/migrate_test.git/foobar"
	urlHTTPWithBadPath          = "https://github.com/mattes/migrate_test.git/foobar"
	urlInvalidSSH               = "ssh://github.com/mattes/migrate_test.git"
	urlValidHTTP                = "https://github.com/mattes/migrate_test.git"
	urlValidHTTPWithUser        = "https://joe@github.com/mattes/migrate_test.git"
	urlValidHTTPWithUserAndPass = "https://joe:shhh@github.com/mattes/migrate_test.git"
	urlValidSSH                 = "ssh://joe@github.com/mattes/migrate_test.git?ssh-key-path="
	urlInvalidSchemeProtocol    = ":foo.bar"
	urlInvalidSchemeType        = "foo://foo.bar/mattes/migrate_test.git"
	urlInvalid                  = "foo.bar"
	schemeUnsupported           = "unsupported"
	valueOwner                  = "mattes"
	valueRepo                   = "migrate_test.git"
	valueHost                   = "github.com"
	valueUser                   = "git"
)

func newRepo() (*git.Repository, error) {

	fs := memfs.New()

	r, err := git.Clone(memory.NewStorage(), fs, &git.CloneOptions{
		URL: urlSSHWithoutPath,
	})

	if err != nil {
		return nil, err
	}

	return r, nil
}

func TestViaSSH(t *testing.T) {
	b := &Git{}
	d, err := b.Open(urlSSH)
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

func TestViaHTTP(t *testing.T) {
	b := &Git{}
	d, err := b.Open(urlHTTP)
	if err != nil {
		t.Fatal(err)
	}

	st.Test(t, d)
}

func TestWithInstance(t *testing.T) {

	vr, err := newRepo()
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{}

	testCases := []struct {
		Expect error
		Repo   *git.Repository
	}{
		{Repo: &git.Repository{}, Expect: git.ErrIsBareRepository},
		{Repo: vr},
	}

	for i, tc := range testCases {
		_, err := WithInstance(tc.Repo, &cfg)
		if err != tc.Expect {
			t.Error(caseNumber(i, haveWant(err, tc.Expect)))
		}
	}
}

func TestRepoMeta_FullRepoName(t *testing.T) {

	testCases := []struct {
		Meta       RepoMeta
		Expect     error
		ExpectName string
	}{
		{Meta: RepoMeta{}, Expect: errOwnerMissing},
		{Meta: RepoMeta{Owner: valueOwner}, Expect: errRepoMissing},
		{Meta: RepoMeta{Owner: valueOwner, Repo: valueRepo}, ExpectName: fmt.Sprintf("%v/%v", valueOwner, valueRepo)},
	}

	for _, tc := range testCases {
		name, err := tc.Meta.FullRepoName()
		if err != tc.Expect {
			t.Error(haveWant(err, tc.Expect))
		}
		if name != tc.ExpectName {
			t.Error(haveWant(name, tc.ExpectName))
		}
	}
}

func TestRepoMeta_URL(t *testing.T) {

	testCases := []struct {
		Meta       RepoMeta
		Expect     error
		ExpectName string
	}{
		{Meta: RepoMeta{}, Expect: errOwnerMissing},
		{Meta: RepoMeta{Owner: valueOwner}, Expect: errRepoMissing},
		{Meta: RepoMeta{Owner: valueOwner, Repo: valueRepo}, Expect: errHostMissing},
		{Meta: RepoMeta{Owner: valueOwner, Repo: valueRepo, Scheme: schemeSSH}, Expect: errHostMissing},
		{Meta: RepoMeta{Owner: valueOwner, Repo: valueRepo, Scheme: schemeHTTPS}, Expect: errHostMissing},
		{Meta: RepoMeta{Owner: valueOwner, Repo: valueRepo, Host: valueHost}, Expect: errSchemeNotSupported},
		{Meta: RepoMeta{Owner: valueOwner, Repo: valueRepo, Host: valueHost, Scheme: schemeUnsupported}, Expect: errSchemeNotSupported},
	}

	for i, tc := range testCases {
		url, err := tc.Meta.URL()
		if err != tc.Expect {
			t.Error(label("error", caseNumber(i, haveWant(err, tc.Expect))))
		}
		if url != tc.ExpectName {
			t.Error(label("url", caseNumber(i, haveWant(url, tc.ExpectName))))
		}
	}
}

func TestRepoMeta_Validate(t *testing.T) {

	testCases := []struct {
		Meta   RepoMeta
		Expect error
	}{
		{Expect: errSchemeNotSupported},
		{Expect: errUserMissing, Meta: RepoMeta{Scheme: schemeSSH}},
		{Expect: errOwnerMissing, Meta: RepoMeta{Scheme: schemeSSH, User: valueUser, Host: valueHost}},
		{Expect: errRepoMissing, Meta: RepoMeta{Scheme: schemeSSH, User: valueUser, Host: valueHost, Owner: valueOwner}},
		{Meta: RepoMeta{Scheme: schemeSSH, User: valueUser, Host: valueHost, Owner: valueOwner, Repo: valueRepo}},
		{Expect: errHostMissing, Meta: RepoMeta{Scheme: schemeHTTPS}},
		{Expect: errOwnerMissing, Meta: RepoMeta{Scheme: schemeHTTPS, Host: valueHost}},
		{Expect: errRepoMissing, Meta: RepoMeta{Scheme: schemeHTTPS, Host: valueHost, Owner: valueOwner}},
		{Meta: RepoMeta{Scheme: schemeHTTPS, Host: valueHost, Owner: valueOwner, Repo: valueRepo}},
	}

	for i, tc := range testCases {

		if have, want := tc.Meta.Validate(), tc.Expect; have != want {
			t.Error(caseNumber(i, haveWant(have, want)))
		}
	}

}

func TestParse(t *testing.T) {

	testCases := []struct {
		URL        string
		Expect     string
		ExpectMeta *RepoMeta
	}{
		{Expect: "missing protocol scheme", URL: urlInvalidSchemeProtocol},
		{Expect: errInvalidURL.Error(), URL: urlInvalid},
		{Expect: errUserMissing.Error(), URL: urlInvalidSSH},
		{URL: urlValidHTTP, ExpectMeta: &RepoMeta{Scheme: schemeHTTPS, Host: valueHost, Owner: valueOwner, Repo: valueRepo}},
		{URL: urlValidHTTPWithUser},
		{URL: urlValidHTTPWithUserAndPass},
		{URL: urlValidSSH},
	}

	for i, tc := range testCases {
		meta, err := Parse(tc.URL)
		if !isError(err, tc.Expect) {
			t.Error(label("error", caseNumber(i, haveWant(err, tc.Expect))))
		}
		if meta != nil && tc.ExpectMeta != nil {
			if meta.Scheme != tc.ExpectMeta.Scheme {
				t.Error(label("meta scheme", caseNumber(i, haveWant(meta.Scheme, tc.ExpectMeta.Scheme))))
			}
			if meta.Host != tc.ExpectMeta.Host {
				t.Error(label("meta host", caseNumber(i, haveWant(meta.Host, tc.ExpectMeta.Host))))
			}
			if meta.User != tc.ExpectMeta.User {
				t.Error(label("meta user", caseNumber(i, haveWant(meta.User, tc.ExpectMeta.User))))
			}
			if meta.Password != tc.ExpectMeta.Password {
				t.Error(label("meta password", caseNumber(i, haveWant(meta.Password, tc.ExpectMeta.Password))))
			}
			if meta.Owner != tc.ExpectMeta.Owner {
				t.Error(label("meta owner", caseNumber(i, haveWant(meta.Owner, tc.ExpectMeta.Owner))))
			}
			if meta.Repo != tc.ExpectMeta.Repo {
				t.Error(label("meta repo", caseNumber(i, haveWant(meta.Repo, tc.ExpectMeta.Repo))))
			}
		}
	}
}

func TestNewRepository(t *testing.T) {

	testCases := []struct {
		URL        string
		Expect     error
		ExpectPath string
	}{
		{URL: urlInvalid, Expect: errInvalidURL},
		{URL: urlValidHTTP},
		{URL: urlValidHTTPWithUser},
		{URL: urlValidHTTPWithUserAndPass},
		{URL: urlValidSSH},
		{Expect: errSchemeNotSupported, URL: urlInvalidSchemeType},
	}

	for i, tc := range testCases {
		_, path, err := NewRepository(tc.URL)
		if err != tc.Expect {
			t.Error(label("error", caseNumber(i, haveWant(err, tc.Expect))))
		}
		if path != tc.ExpectPath {
			t.Error(label("path", caseNumber(i, haveWant(path, tc.ExpectPath))))
		}
	}
}

func TestGit_Open(t *testing.T) {

	testCases := []struct {
		URL    string
		Expect error
	}{
		{URL: urlInvalid, Expect: errInvalidURL},
		{URL: urlSSH},
		{URL: urlHTTP},
		{URL: urlHTTPWithBadPath, Expect: os.ErrNotExist},
		{URL: urlSSHWithBadPath, Expect: os.ErrNotExist},
	}

	g := &Git{}

	for i, tc := range testCases {
		_, err := g.Open(tc.URL)
		if err != tc.Expect {
			t.Error(caseNumber(i, haveWant(err, tc.Expect)))
		}
	}

}

func label(label string, message string) string {
	return fmt.Sprintf("%s: %s", label, message)
}

func caseNumber(index int, message string) string {
	return fmt.Sprintf("test case :%v: %v", index+1, message)
}

func haveWant(have, want interface{}) string {
	return fmt.Sprintf("an unexpected result was returned (have: %v, want: %v)", have, want)
}

func isError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	matched, mErr := regexp.MatchString(re, err.Error())
	if mErr != nil {
		return false
	}
	return matched
}
