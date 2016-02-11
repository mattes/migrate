# Cassandra Driver

## Usage

```bash
migrate -url cassandra://host:port/keyspace -path ./db/migrations create add_field_to_table
migrate -url cassandra://host:port/keyspace -path ./db/migrations up
CASSANDRA_CA_PATH=my_cassandra_ca.pem migrate -url cassandra://host:port/keyspace -path ./db/migrations up # Using encrypted connection
migrate help # for more info
```

## Authors

* Paul Bergeron, https://github.com/dinedal
* Johnny Bergström, https://github.com/balboah
* pateld982, http://github.com/pateld982