package neo4j

import (
	"fmt"
	"testing"
	"database/sql"
	sqldriver "database/sql/driver"

	dt "github.com/mattes/migrate/database/testing"
	mt "github.com/mattes/migrate/testing"
)

var versions = []mt.Version{
	{Image: "neo4j:3", ENV: []string{"x-migrations-label=SchemaMigrationTest"}},
}

func isReady(i mt.Instance) bool {
	db, err := sql.Open("neo4j-cypher", fmt.Sprintf("http://%v:%v", i.Host(), i.Port()))
	if err != nil {
		return false
	}
	defer db.Close()
	err = db.Ping()

	if err == sqldriver.ErrBadConn {
		return false
	}

	return true
}

func Test(t *testing.T) {

	mt.ParallelTest(t, versions, isReady,
		func(t *testing.T, i mt.Instance) {
			p := &Neo4j{}
			addr := fmt.Sprintf("http://%v:%v", i.Host(), i.Port())
			d, err := p.Open(addr)
			if err != nil {
				t.Fatalf("%v", err)
			}
			dt.Test(t, d, []byte("CREATE (:Test)"))
		})
}
