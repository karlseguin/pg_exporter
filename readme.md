# Prometheus PostgreSQL Exporter

The goal of this exporter is to export database and table metrics. This is different than the community postgresql exporter which [struggles with table metrics](https://github.com/prometheus-community/postgres_exporter/issues/296)

```
    pg_exporter -h 127.0.0.1 -p 5432 -u user -passwordFile=/path/to/pw \
       -exclude "db1,db2,db2:tblX,db3:tblY" -listen 0.0.0.0:9187 -logLevel error
```

## Command Line Arguments

* `-h HOST` - postgresql host to connect to (defaults to 127.0.1)
* `-p POST` - postgresql port to connect to (defaults to 5432)
* `-u USER` - postgresql user to use
* `-d DATABASE` - main database to connect to (defaults to postgres)
* `-passwordFile PATH` - path to the file containing the password
* `-exclude LIST` - database(s) and/or table(s) to exclude
* `-listen IP:PORT` - address to listen on (defaults to 127.0.0.1:9187)
* `-path PATH` - path where metrics are avaialble (defaults to /)
* `-prefix PREFIX` - prefixes appended to metrics (defaults to pg_)
* `-noGoStats` - don't include go metrics about the pg_exporter process itself
* `-noProcessStats` - don't include process metrics about the pg_exporter process itself
* `-minRows` - ignore tables with less rows than the specified amount (defaults to 0)
