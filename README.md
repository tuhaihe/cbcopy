# cbcopy

cbcopy is designed to migrate Greenplum Database clusters, including both metadata and data, to Cloudberry Database clusters. It is also used for data migration between different Cloudberry Database clusters for disaster recovery and specific version upgrades.

## How does cbcopy work?

### Metadata migration
The metadata migration feature of cbcopy is derived from gpbackup and gprestore. Its main advantage over the built-in pg\_dump of GPDB is that it uses batch retrieval of metadata, rather than fetching metadata one row at a time like pg\_dump. This results in a significant performance advantage when migrating metadata in scenarios where the volume of metadata in the source database is large compared to pg_dump.

### Data migration

Both GPDB and CBDB support starting programs via SQL commands, and cbcopy utilizes this feature. During data migration, it uses SQL commands to start a program on the target database to receive and load data, while simultaneously using SQL commands to start a program on the source database to unload data and send it to the program on the target database.

## Pre-Requisites

The project requires the Go Programming language version 1.19 or higher. Follow the directions [here](https://golang.org/doc/) for installation, usage and configuration instructions.

## Downloading

```bash
go get TBD
```

## Building and installing binaries
Switch your current working directory to the above `cbcopy` source directory

**Build**

```bash
make
```
This will build the cbcopy and cbcopy_helper binaries in the source directory.

**Install**

```bash
make install
```
This will install the cbcopy and cbcopy_helper programs binaries the $GPHOME/bin directory. Note that GPDB must be sourced for this to work.

## Test
```bash
make test
```
Runs the unit tests


## Migrating Data with cbcopy

Before migrating data, you need to copy cbcopy_helper to the $GPHOME/bin directory on all nodes of both the source and target databases. Then you need to find a host that can connect to both the source database and the target database, and use the cbcopy command on that host to initiate the migration.By default, both metadata and data are migrated.


### Migration Modes

cbcopy supports seven migration modes.

- `--full` - Migrate all metadata and data from the source database to the target database.
- `--dbname` - Migrate a specific database or multiple databases from the source to the target database.
- `--schema` - Migrate a specific schema or multiple schemas from the source database to the target database.
-  `--schema-mapping-file` - Migrate specific schemas specified in a file from the source database to the target database.
- `--include-table` - Migrate specific tables or multiple tables from the source database to the target database.
- `--include-table-file` - Migrate specific tables specified in a file from the source database to the target database.
- `--global-metadata-only` - Migrate global objects from the source database to the target database.


### Data Loading Modes
cbcopy supports two data loading modes.

- `--append` - Insert the migrated records into the table directly, regardless of the existing records.
- `--truncate` - First, clear the existing records in the table, and then insert the migrated records into the table.


### Object dependencies

If the tables you are migrating depend on certain global objects, you need to include the --with-globalmeta option (default: false) during the migration; otherwise, the creation of these tables in the target database will fail.

### Role
If you want to change the table owner information during migration, which means you do not want to create the same role in the target database (disable the --with-globalmeta option), you need to first create the corresponding role in the target database. Then, use the --owner-mapping-file to specify the mapping between the source role and the target role; otherwise, the creation of tables in the target database will fail.

### Tablespace
- `--tablespace` - We support migrating all source database objects into a single tablespace on the target database(Other modes besides the --full mode).if you need to migrate database objects from different schemas into different tablespaces, the best practice might be to migrate one schema at a time from the source database


### Parallel Jobs

- `--copy-jobs` - The maximum number of tables that concurrently copies.


### Validate Migration
During migration, we will compare the number of rows returned by COPY TO from the source database (i.e., the number of records coming out of the source database) with the number of rows returned by COPY FROM in the target database (i.e., the number of records loaded in the target database). If the two counts do not match, the migration of that table will fail.

### Copy Strategies

cbcopy internally supports three copy strategies for tables.

- `Copy On Master` - If the table's statistics pg_class->reltuples is less than --on-segment-threshold, cbcopy will enable the Copy On Master strategy for this table, meaning that data migration between the source and target databases can only occur through the master node.

- `Copy On Segment` - If the table's statistics pg_class->reltuples is greater than --on-segment-threshold, and both the source and target databases have the same version and the same number of nodes, cbcopy will enable the Copy On Segment strategy for this table. This means that data migration between the source and target databases will occur in parallel across all segment nodes without data redistribution.
- `Copy on External Table` - For tables that do not meet the conditions for the above two strategies, cbcopy will enable the Copy On External Table strategy. This means that data migration between the source and target databases will occur in parallel across all segment nodes with data redistribution.

## Examples
```
cbcopy --with-globalmeta --source-host=127.0.0.1 --source-port=45432 --source-user=gpadmin --dest-host=127.0.0.1 --dest-port=55432 --dest-user=cbdb --dbname=testdb --truncate
```

## cbcopy reference
```
cbcopy utility for migrating data from Greenplum Database (GPDB) to Cloudberry Database (CBDB)

Usage:
  cbcopy [flags]

Flags:
      --analyze                      Analyze tables after copy
      --append                       Append destination table if it exists
      --compression                  Transfer the compression data, instead of the plain data
      --copy-jobs int                The maximum number of tables that concurrently copies, valid values are between 1 and 64512 (default 4)
      --data-only                    Only copy data, do not copy metadata
      --data-port-range string       The range of listening port number to choose for receiving data on dest cluster (default "1024-65535")
      --dbname strings               The database(s) to be copied, separated by commas
      --debug                        Print debug log messages
      --dest-dbname strings          The database(s) in destination cluster to copy to, separated by commas
      --dest-host string             The host of destination cluster
      --dest-port int                The port of destination cluster (default 5432)
      --dest-schema strings          The schema(s) in destination database to copy to, separated by commas
      --dest-table strings           The renamed dest table(s) for include-table, separated by commas
      --dest-table-file string       The renamed dest table(s) for include-table-file, The line format is "dbname.schema.table"
      --dest-user string             The user of destination cluster (default "gpadmin")
      --exclude-table strings        Copy all tables except the specified table(s), separated by commas
      --exclude-table-file string    Copy all tables except the specified table(s) listed in the file, The line format is "dbname.schema.table"
      --full                         Copy full data cluster
      --global-metadata-only         Only copy global metadata, do not copy data
      --help                         Print help info and exit
      --include-table strings        Copy only the specified table(s), separated by commas, in the format database.schema.table
      --include-table-file string    Copy only the specified table(s) listed in the file, The line format is "dbname.schema.table"
      --ip-mapping-file string       ip mapping file (format, ip1:ip2)
      --metadata-jobs int            The maximum number of metadata restore tasks, valid values are between 1 and 64512 (default 2)
      --metadata-only                Only copy metadata, do not copy data
      --on-segment-threshold int     Copy between masters directly, if the table has smaller or same number of rows (default 1000000)
      --owner-mapping-file string    Object owner mapping file, The line format is "source_role_name,dest_role_name"
      --quiet                        Suppress non-warning, non-error log messages
      --schema strings               The schema(s) to be copied, separated by commas, in the format database.schema
      --schema-mapping-file string   Schema mapping file, The line format is "source_dbname.source_schema,dest_dbname.dest_schema"
      --source-host string           The host of source cluster (default "127.0.0.1")
      --source-port int              The port of source cluster (default 5432)
      --source-user string           The user of source cluster (default "gpadmin")
      --statistics-file string       Table statistics file
      --statistics-jobs int          The maximum number of collecting statistics tasks, valid values are between 1 and 64512 (default 4)
      --statistics-only              Only collect statistics of source cluster and write to file
      --tablespace string            Create objects in this tablespace
      --truncate                     Truncate destination table if it exists prior to copying data
      --validate                     Perform data validation when copy is complete (default true)
      --verbose                      Print verbose log messages
      --version                      Print version number and exit
      --with-globalmeta              Copy global meta objects (default: false)
```
