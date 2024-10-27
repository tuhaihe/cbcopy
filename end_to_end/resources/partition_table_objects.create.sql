create table public.tb_partition(
    id int,
    name varchar,
    dt date,
    count int)
distributed by (id)
partition by list (dt)
(partition p1 values ('2023-07-01'),
    partition p2 values ('2023-07-15'),
    default partition l1def
)
;

insert into tb_partition values (1, 'aaa', '2023-07-15', 100),(2, 'aaa', '2023-07-01', 200),(3, 'aaa', '2023-08-15', 300);

/*
analyze rootpartition tb_partition;

cbcopy does not do analyze by default, so here we don't do analyze so we could use same expect output file.

*/

-- clean: drop table public.tb_partition;

-- check
/*

db1=# \d+ tb_partition
                                      Partitioned table "public.tb_partition"
 Column |       Type        | Collation | Nullable | Default | Storage  | Compression | Stats target | Description
--------+-------------------+-----------+----------+---------+----------+-------------+--------------+-------------
 id     | integer           |           |          |         | plain    |             |              |
 name   | character varying |           |          |         | extended |             |              |
 dt     | date              |           |          |         | plain    |             |              |
 count  | integer           |           |          |         | plain    |             |              |
Partition key: LIST (dt)
Partitions: tb_partition_1_prt_p1 FOR VALUES IN ('2023-07-01'),
            tb_partition_1_prt_p2 FOR VALUES IN ('2023-07-15'),
            tb_partition_1_prt_l1def DEFAULT
Distributed by: (id)

(pg14 on 3x)
db1=# \d+ tb_partition
ERROR:  column "attrnums" does not exist
LINE 1: SELECT attrnums
               ^
                                     Table "public.tb_partition"
 Column |       Type        | Collation | Nullable | Default | Storage  | Stats target | Description
--------+-------------------+-----------+----------+---------+----------+--------------+-------------
 id     | integer           |           |          |         | plain    |              |
 name   | character varying |           |          |         | extended |              |
 dt     | date              |           |          |         | plain    |              |
 count  | integer           |           |          |         | plain    |              |
Child tables: tb_partition_1_prt_l1def,
              tb_partition_1_prt_p1,
              tb_partition_1_prt_p2
Tablespace: "dfs_tablespace"
Options: appendonly=true, blocksize=1048576

db1=# select count(*) from tb_partition; select count(*) from tb_partition_1_prt_p1; select count(*) from tb_partition_1_prt_p2; select count(*) from tb_partition_1_prt_l1def;
 count
-------
     3
(1 row)

 count
-------
     1
(1 row)

 count
-------
     1
(1 row)

 count
-------
     1
(1 row)

 */