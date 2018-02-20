# Neo4J

`http://user:password@host:port`

## Use with existing client

```go

import (
	"log"
	"github.com/mattes/migrate"
	"github.com/mattes/migrate/database/neo4j"
	_ "github.com/mattes/migrate/source/file"
	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

func main() {

	boltDriver := bolt.NewDriver()
	conn, err := boltDriver.OpenNeo("bolt://neo4j:root@localhost:7687")
	if err != nil {
		panic(err)
	}
	defer conn.Close()


	driver, err := neo4j.WithInstance(conn, &neo4j.Config{})
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
