# Firebird Driver

* Runs migrations in transcations.
  That means that if a migration failes, it will be safely rolled back.
* Tries to return helpful error messages.
* Stores migration version details in table ``schema_migrations``.
  This table will be auto-generated.


## Usage

```bash
migrate -url firebirdsql://migrateuser:migratepass@firebirdsql:3050/databases/migratedb -path ./db/migrations create add_field_to_table
migrate -url firebirdsql://migrateuser:migratepass@firebirdsql:3050/databases/migratedb -path ./db/migrations up
migrate help # for more info
```

## Authors

* Leonardo Saraiva, https://github.com/vyper
