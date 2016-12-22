# PostgreSQL Driver

* Runs migrations in transactions: if a migration fails it will be rolled back safely.
* Tries to return helpful error messages.
* Stores migration version details in table ``schema_migrations``. This table will be automatically created if it does not exist.

## Usage

```bash
migrate -url postgres://user@host:port/database -path ./db/migrations create add_field_to_table
migrate -url postgres://user@host:port/database -path ./db/migrations up
migrate help # for more info

# TODO(mattes): thinking about adding some custom flag to allow migration within schemas:
-url="postgres://user@host:port/database?schema=name"

# see more docs: https://godoc.org/github.com/lib/pq#hdr-Connection_String_Parameters
```

## Authors

* Matthias Kadenbach, https://github.com/mattes
