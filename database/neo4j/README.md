# Neo4J

`http://user:password@host:port?query`

| URL Query  | WithInstance Config | Description |
|------------|---------------------|-------------|
| `x-migrations-label` | `SchemaMigration` | Name of the migrations node |

## Use with existing client

```go

import (
	"log"
	"github.com/mattes/migrate"
	"github.com/mattes/migrate/database/neo4j"
	"database/sql"

	_ "github.com/mattes/migrate/source/file"
	_ "gopkg.in/cq.v1"
)

func main() {

	db, err := sql.Open("neo4j-cypher", "http://neo4j:password@localhost:7474")
	if err != nil {
		log.Fatal(err)
	}

	driver, err := neo4j.WithInstance(db, &neo4j.Config{})
	if err != nil {
		panic(err)
	}

	migration, err := migrate.NewWithDatabaseInstance(
		"file:///migrations",
		"", driver)
	if err != nil {
		panic(err)
	}

	err = migration.Up()
	if err != nil {
		panic(err)
	}
}
```
