-- 库
-- 模式/命名空间
-- 表 -- 外部表 (EXTERNAL TABLE, FOREIGN TABLE/SERVER/USER MAPPING/FOREIGN DATA WRAPPER) -- 表空间 TABLESPACE
-- 函数 (FUNCTION, PROCEDURE)
-- 协议
-- 扩展
-- 排序规则
-- 类型
-- 过程语言
-- 变换
-- 文本搜索
-- 操作符 (OPERATOR, OPERATOR CLASS, OPERATOR FAMILY)
-- 聚集函数
-- 类型转换
-- 访问方法
-- 视图 (VIEW, MATERIALIZED VIEW)
-- 序列
-- 约束 -- 域 (DOMAIN)
-- 字符集转换
-- 索引
-- 规则
-- 触发器
-- 权限 (ROLE, PRIVILEGES, GROUP, USER)
-- 行安全策略
-- 资源组/队列 (RESOURCE GROUP, RESOURCE QUEUE)
-- 扩展统计信息

-- table with all types
-- partition table


-- https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-sql_ref.html


--/ CREATE ACCESS METHOD
--/ CREATE AGGREGATE
--/ CREATE CAST
--/ CREATE COLLATION
--/ CREATE CONVERSION
--/ CREATE DATABASE
--/ CREATE DOMAIN
--/ CREATE EXTENSION
--/ CREATE EXTERNAL TABLE
--/ CREATE FOREIGN DATA WRAPPER
--/ CREATE FOREIGN TABLE
--/ CREATE FUNCTION
--/ CREATE GROUP
--/ CREATE INDEX
--/ CREATE LANGUAGE
--/ CREATE MATERIALIZED VIEW
--/ CREATE OPERATOR
--/ CREATE OPERATOR CLASS
--/ CREATE OPERATOR FAMILY
--/ CREATE POLICY
--/ CREATE PROCEDURE
--/ CREATE PROTOCOL
--/ CREATE RESOURCE GROUP
--/ CREATE RESOURCE QUEUE
--/ CREATE ROLE
--/ CREATE RULE
--/ CREATE SCHEMA
--/ CREATE SEQUENCE
--/ CREATE SERVER
--/ CREATE STATISTICS
--/ CREATE TABLE
--/ CREATE TABLE AS
--/ CREATE TABLESPACE
--/ CREATE TEXT SEARCH CONFIGURATION
--/ CREATE TEXT SEARCH DICTIONARY
--/ CREATE TEXT SEARCH PARSER
--/ CREATE TEXT SEARCH TEMPLATE
--/ CREATE TYPE
--/ CREATE USER
--/ CREATE USER MAPPING
--/ CREATE VIEW

-----------------------------------

-- 库

-- 模式/命名空间
-- 表 -- 外部表 (EXTERNAL TABLE, FOREIGN TABLE/SERVER/USER MAPPING/FOREIGN DATA WRAPPER) -- 表空间 TABLESPACE

CREATE SCHEMA schema1;
CREATE SCHEMA schema2;

CREATE TABLE t1 (c1 int, c2 varchar);
CREATE TABLE schema1.t1 (c1 int, c2 varchar);
CREATE TABLE schema2.t1 (c1 int, c2 varchar);

INSERT INTO t1 VALUES (1, 'public'),(2, 'public'),(3, 'public');
INSERT INTO schema1.t1 VALUES (1, 'schema1'),(2, 'schema1'),(3, 'schema1');
INSERT INTO schema2.t1 VALUES (1, 'schema2'),(2, 'schema2'),(3, 'schema2');

-- clean: DROP TABLE schema2.t1; DROP TABLE schema1.t1; DROP TABLE t1; DROP SCHEMA schema2 CASCADE; DROP SCHEMA schema1 CASCADE;

-- check
/*

db_test1=# \dn schema*
  List of schemas
  Name   |  Owner
---------+---------
 schema1 | gpadmin
 schema2 | gpadmin
(2 rows)

db_test1=# select * from t1 order by c1; select * from schema1.t1 order by c1; select * from schema2.t1 order by c1;
 c1 |   c2
----+--------
  1 | public
  2 | public
  3 | public
(3 rows)

 c1 |   c2
----+---------
  1 | schema1
  2 | schema1
  3 | schema1
(3 rows)

 c1 |   c2
----+---------
  1 | schema2
  2 | schema2
  3 | schema2
(3 rows)

 */

-- CREATE EXTERNAL TABLE

CREATE EXTERNAL TABLE ext_expenses (name text, date date,
    amount float4, category text, description text)
    LOCATION (
        'file://seghost1/dbfast/external/expenses1.csv'
        )
    FORMAT 'CSV' ( HEADER );

/*

CREATE EXTERNAL TABLE ext_expenses3 (name text, date date,
    amount float4, category text, description text)
    LOCATION (
        'file://seghost1/dbfast/external/expenses1.csv'
        )
    FORMAT 'CSV' ( HEADER )
LOG ERRORS PERSISTENTLY
SEGMENT REJECT LIMIT 10 PERCENT
;

 logerrors |  oid  |                   location                    |   execlocation   | formattype |                    formatopts                     |                                                                                                  command
                                                                                        | rejectlimit | rejectlimittype | logerrors | logerrpersist | encoding | writable
-----------+-------+-----------------------------------------------+------------------+------------+---------------------------------------------------+-------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------+-------------+-----------------+-----------+---------------+----------+----------
  p         | 85080 | file://seghost1/dbfast/external/expenses1.csv | ALL_SEGMENTS     | c          | delimiter ',' null '' escape '"' quote '"' header |

cbcopy error

[CRITICAL]:-sql: Scan error on column index 8, name "logerrors": sql/driver: couldn't convert "p" into type bool

https://code.hashdata.xyz/cloudberry/cbdb/-/issues/461

let's remove 'LOG ERRORS PERSISTENTLY' clause in test first to workaround before this issue fix.

 */

-- clean: DROP EXTERNAL TABLE ext_expenses;

-- check
/*

db_test1=# \dE ext_expenses;
                List of relations
 Schema |     Name     |     Type      |  Owner
--------+--------------+---------------+---------
 public | ext_expenses | foreign table | gpadmin
(1 row)

if using cbdb psql connect 3x server, it does not return
db1=#  \dE ext_expenses;
Did not find any relation named "ext_expenses".

maybe it's because pg14 vs pg9 system table content diff (from test):
pg9:  pg_exttable - external table, pg_foreign_table - foreign table
pg14: pg_exttable - external table, pg_foreign_table - foreign table + external table

so, add one more check in another way until \xx command fix

db1=# select relname, urilocation from pg_exttable join pg_class on pg_exttable.reloid=pg_class.oid and pg_class.relname='ext_expenses';
   relname    |                   urilocation
--------------+-------------------------------------------------
 ext_expenses | {file://seghost1/dbfast/external/expenses1.csv}
(1 row)

 */

-- CREATE FOREIGN DATA WRAPPER
-- CREATE SERVER
-- CREATE FOREIGN TABLE
-- CREATE USER MAPPING

CREATE FOREIGN DATA WRAPPER fdw;

CREATE SERVER sc FOREIGN DATA WRAPPER fdw;

-- cbdb ok, but 3x failed with "ERROR:  role "testrole" does not exist", so create this role first
CREATE ROLE testrole;

CREATE USER MAPPING FOR testrole
    SERVER sc
    OPTIONS (password 'password', user 'foreign_user');

CREATE FOREIGN TABLE ft1 (
    c1 integer OPTIONS (param1 'val1') NOT NULL,
    c2 text OPTIONS (param2 'val2', param3 'val3')
    ) SERVER sc OPTIONS (delimiter ',', quote '"');

-- clean: DROP FOREIGN TABLE ft1; DROP USER MAPPING FOR testrole SERVER sc; DROP SERVER sc; DROP FOREIGN DATA WRAPPER fdw;

-- check
/*

db_test1=# \dE ft1
            List of relations
 Schema | Name |     Type      |  Owner
--------+------+---------------+---------
 public | ft1  | foreign table | gpadmin
(1 row)

db_test1=# \des sc
        List of foreign servers
 Name |  Owner  | Foreign-data wrapper
------+---------+----------------------
 sc   | gpadmin | fdw
(1 row)

db_test1=# \deu+
                       List of user mappings
 Server | User name |                 FDW options
--------+-----------+----------------------------------------------
 sc     | testrole  | (password 'password', "user" 'foreign_user')
(1 row)

gp5

ERROR:  syntax error at or near "FOREIGN"
LINE 60: CREATE FOREIGN DATA WRAPPER fdw;
                ^

ERROR:  syntax error at or near "SERVER"
LINE 1: CREATE SERVER sc FOREIGN DATA WRAPPER fdw;
               ^

ERROR:  syntax error at or near "FOR"
LINE 1: CREATE USER MAPPING FOR testrole
                            ^

ERROR:  syntax error at or near "FOREIGN"
LINE 1: CREATE FOREIGN TABLE ft1 (
               ^

psql14

\dE ft1
psql:resources/all_types_objects.check.sql:5: error: Did not find any relation named "ft1".
\des sc
psql:resources/all_types_objects.check.sql:6: error: The server (version 8.3) does not support foreign servers.
\deu+
psql:resources/all_types_objects.check.sql:7: error: The server (version 8.3) does not support user mappings.

 */

-- CREATE TABLESPACE

-- $ mkdir /tmp/tbs1 /tmp/tbs0 /tmp/tbsz
CREATE TABLESPACE mytblspace LOCATION '/tmp/tbsz' WITH (content0='/tmp/tbs0', content1='/tmp/tbs1');

-- clean: DROP TABLESPACE mytblspace;

-- check
/*

db_test1=# \db mytblspace
       List of tablespaces
    Name    |  Owner  | Location
------------+---------+-----------
 mytblspace | gpadmin | /tmp/tbsz
(1 row)

3x,

db1=# CREATE TABLESPACE mytblspace LOCATION '/tmp/tbsz' WITH (content0='/tmp/tbs0', content1='/tmp/tbs1');
ERROR:  option "content0" not recognized (tablespace_hdw.c:228)

db1=# CREATE TABLESPACE mytblspace LOCATION '/tmp/tbsz';
ERROR:  create pg_tablespace "mytblspace" is not supported. (tablespace.c:424)
db1=#

db1=# \db mytblspace
   List of tablespaces
 Name | Owner | Location
------+-------+----------
(0 rows)

gp5

db1=# CREATE TABLESPACE mytblspace LOCATION '/tmp/tbsz' WITH (content0='/tmp/tbs0', content1='/tmp/tbs1');
ERROR:  syntax error at or near "LOCATION"
LINE 1: CREATE TABLESPACE mytblspace LOCATION '/tmp/tbsz' WITH (cont...


 */

-- 函数 (FUNCTION, PROCEDURE)

CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'select $1 + $2;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

-- clean: DROP FUNCTION add;

-- check
/*

db_test1=# \df add
                       List of functions
 Schema | Name | Result data type | Argument data types | Type
--------+------+------------------+---------------------+------
 public | add  | integer          | integer, integer    | func
(1 row)

 */

-- CREATE PROCEDURE

CREATE PROCEDURE insert_data(a integer, b integer)
LANGUAGE SQL
AS $$
--INSERT INTO tbl VALUES (a);
--INSERT INTO tbl VALUES (b);
$$;

-- clean: DROP PROCEDURE insert_data;

-- check
/*

db_test1=# \df insert_data
                              List of functions
 Schema |    Name     | Result data type |    Argument data types     | Type
--------+-------------+------------------+----------------------------+------
 public | insert_data |                  | IN a integer, IN b integer | proc
(1 row)

GP6/PG9 does not have create procedure

db1=# CREATE PROCEDURE insert_data(a integer, b integer)
LANGUAGE SQL
AS $$
--INSERT INTO tbl VALUES (a);
--INSERT INTO tbl VALUES (b);
$$;

ERROR:  syntax error at or near "PROCEDURE"
LINE 1: CREATE PROCEDURE insert_data(a integer, b integer)
               ^
 */

-- 协议

CREATE FUNCTION f1() RETURNS int AS 'SELECT 1' LANGUAGE SQL;
CREATE PROTOCOL p1 (readfunc = f1);

-- clean: DROP PROTOCOL p1; DROP FUNCTION f1;

-- check
/*

db_test1=# select count(*) from pg_extprotocol where ptcname='p1';
 count
-------
     1
(1 row)

 */

-- 扩展

CREATE EXTENSION hstore;

-- clean: DROP EXTENSION hstore;

-- check
/*

db_test1=# \dx hstore
                         List of installed extensions
  Name  | Version | Schema |                   Description
--------+---------+--------+--------------------------------------------------
 hstore | 1.8     | public | data type for storing sets of (key, value) pairs
(1 row)

3x

db1=# \dx hstore
                         List of installed extensions
  Name  | Version | Schema |                   Description
--------+---------+--------+--------------------------------------------------
 hstore | 1.3     | public | data type for storing sets of (key, value) pairs
(1 row)

gp5

ERROR:  could not open extension control file "/usr/local/greenplum-db-5.29.11/share/postgresql/extension/hstore.control"

 */

-- 排序规则

CREATE COLLATION collation_french (LOCALE = 'fr_FR.utf8');

-- clean: DROP COLLATION collation_french;

-- check
/*

db_test1=# \dO collation_french
                               List of collations
 Schema |       Name       |  Collate   |   Ctype    | Provider | Deterministic?
--------+------------------+------------+------------+----------+----------------
 public | collation_french | fr_FR.utf8 | fr_FR.utf8 | libc     | yes
(1 row)

gp5

ERROR:  syntax error at or near "COLLATION"
LINE 21: CREATE COLLATION collation_french (LOCALE = 'fr_FR.utf8');
                ^

psql14

psql:resources/all_types_objects.check.sql:13: error: The server (version 8.3) does not support collations.

 */

-- 类型

CREATE TYPE type_mood AS ENUM ('sad', 'ok', 'happy');

-- clean: DROP TYPE type_mood;

-- check
/*

db_test1=# \dT type_mood
        List of data types
 Schema |   Name    | Description
--------+-----------+-------------
 public | type_mood |
(1 row)

gp4

db1=# CREATE TYPE type_mood AS ENUM ('sad', 'ok', 'happy');
ERROR:  syntax error at or near "ENUM"
LINE 1: CREATE TYPE type_mood AS ENUM ('sad', 'ok', 'happy');
                                 ^

 */

-- 过程语言

CREATE FUNCTION plperl_call_handler() RETURNS
    language_handler
AS '$libdir/plperl'
    LANGUAGE C;

CREATE PROCEDURAL LANGUAGE plperlabc HANDLER plperl_call_handler;

-- clean: DROP PROCEDURAL LANGUAGE plperlabc; DROP FUNCTION plperl_call_handler;

-- check
/*

db_test1=# \dL plperlabc
              List of languages
   Name    |  Owner  | Trusted | Description
-----------+---------+---------+-------------
 plperlabc | gpadmin | f       |
(1 row)

gp5

ERROR:  could not load library "/usr/local/greenplum-db-5.29.11/lib/postgresql/plperl.so": libperl.so: cannot open shared

 */

-- 变换

CREATE TRANSFORM FOR pg_catalog.int4 LANGUAGE c (FROM SQL WITH FUNCTION numeric_support(internal), TO SQL WITH FUNCTION int4recv(internal));

-- clean: DROP TRANSFORM FOR pg_catalog.int4 LANGUAGE c;

-- check
/*

db_test1=# select trffromsql, trftosql from pg_transform order by trffromsql, trftosql;
   trffromsql    | trftosql
-----------------+----------
 numeric_support | int4recv
(1 row)

GP6/PG9 does not have transform

db1=# CREATE TRANSFORM FOR pg_catalog.int4 LANGUAGE c (FROM SQL WITH FUNCTION numeric_support(internal), TO SQL WITH FUNCTION int4recv(internal));
ERROR:  syntax error at or near "TRANSFORM"
LINE 1: CREATE TRANSFORM FOR pg_catalog.int4 LANGUAGE c (FROM SQL WI...
               ^
db1=# select trffromsql, trftosql from pg_transform order by trffromsql, trftosql;
ERROR:  relation "pg_transform" does not exist
LINE 1: select trffromsql, trftosql from pg_transform order by trffr...
                                         ^
db1=#

 */

-- 文本搜索

CREATE TEXT SEARCH CONFIGURATION public.testconfiguration(PARSER = pg_catalog."default");

CREATE TEXT SEARCH DICTIONARY public.testdictionary(TEMPLATE = snowball, LANGUAGE = 'russian', STOPWORDS = 'russian');

CREATE TEXT SEARCH PARSER public.testparser(START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);

CREATE TEXT SEARCH TEMPLATE public.testtemplate(LEXIZE = dsimple_lexize);

-- clean: DROP TEXT SEARCH TEMPLATE public.testtemplate; DROP TEXT SEARCH PARSER public.testparser; DROP TEXT SEARCH DICTIONARY public.testdictionary; DROP TEXT SEARCH CONFIGURATION public.testconfiguration;

-- check
/*

db_test1=# \dF testconfiguration
    List of text search configurations
 Schema |       Name        | Description
--------+-------------------+-------------
 public | testconfiguration |
(1 row)

db_test1=# \dFd testdictionary
   List of text search dictionaries
 Schema |      Name      | Description
--------+----------------+-------------
 public | testdictionary |
(1 row)

db_test1=# \dFp testparser
    List of text search parsers
 Schema |    Name    | Description
--------+------------+-------------
 public | testparser |
(1 row)

db_test1=# \dFt testtemplate
    List of text search templates
 Schema |     Name     | Description
--------+--------------+-------------
 public | testtemplate |
(1 row)

gp4

db1=# CREATE TEXT SEARCH CONFIGURATION public.testconfiguration(PARSER = pg_catalog."default");
ERROR:  syntax error at or near "TEXT"
LINE 1: CREATE TEXT SEARCH CONFIGURATION public.testconfiguration(PA...
               ^

 */

-- 操作符 (OPERATOR, OPERATOR CLASS, OPERATOR FAMILY)

-- CREATE OPERATOR CLASS

CREATE OPERATOR CLASS public.testclass FOR TYPE int USING gist AS OPERATOR 1 =, OPERATOR 2 < , FUNCTION 1 abs(integer), FUNCTION 2 int4out(integer);

-- clean: DROP OPERATOR FAMILY public.testclass USING gist; -- (before 5) DROP OPERATOR CLASS public.testclass USING gist;

-- check
/*

db_test1=# \dAc gist integer;
                   List of operator classes
  AM  | Input type | Storage type | Operator class | Default?
------+------------+--------------+----------------+----------
 gist | integer    |              | testclass      | no
(1 row)

db_test1=# \dAf gist integer
         List of operator families
  AM  | Operator family | Applicable types
------+-----------------+------------------
 gist | testclass       | integer
(1 row)

db_test1=# \dAo gist testclass
              List of operators of operator families
  AM  | Operator family |      Operator      | Strategy | Purpose
------+-----------------+--------------------+----------+---------
 gist | testclass       | =(integer,integer) |        1 | search
 gist | testclass       | <(integer,integer) |        2 | search
(2 rows)

db_test1=# \dAp gist testclass
                      List of support functions of operator families
  AM  | Operator family | Registered left type | Registered right type | Number | Function
------+-----------------+----------------------+-----------------------+--------+----------
 gist | testclass       | integer              | integer               |      1 | abs
 gist | testclass       | integer              | integer               |      2 | int4out
(2 rows)

psql14-gp5

todo: change to other way for gp5

\dAc gist integer;
psql:resources/all_types_objects.check.sql:21: ERROR:  function pg_catalog.format(unknown, name) does not exist
LINE 11:     THEN pg_catalog.format('%I', c.opcname)
                  ^
HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
\dAf gist integer
psql:resources/all_types_objects.check.sql:22: ERROR:  function pg_catalog.pg_opfamily_is_visible(oid) does not exist
LINE 4:     WHEN pg_catalog.pg_opfamily_is_visible(f.oid)
                 ^
HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
\dAo gist testclass
psql:resources/all_types_objects.check.sql:23: ERROR:  function pg_catalog.pg_opfamily_is_visible(oid) does not exist
LINE 4:     WHEN pg_catalog.pg_opfamily_is_visible(of.oid)
                 ^
HINT:  No function matches the given name and argument types. You might need to add explicit type casts.
\dAp gist testclass
psql:resources/all_types_objects.check.sql:24: ERROR:  function pg_catalog.pg_opfamily_is_visible(oid) does not exist
LINE 4:     WHEN pg_catalog.pg_opfamily_is_visible(of.oid)
                 ^
HINT:  No function matches the given name and argument types. You might need to add explicit type casts.

 */

-- CREATE OPERATOR FAMILY

CREATE OPERATOR FAMILY public.testfam USING hash;

-- clean: DROP OPERATOR FAMILY public.testfam USING hash;

-- check
/*

db_test1=# select count(*) from pg_opfamily where opfname='testfam';
 count
-------
     1
(1 row)

gp4

postgres=# CREATE OPERATOR FAMILY public.testfam USING hash;
ERROR:  syntax error at or near "public"
LINE 1: CREATE OPERATOR FAMILY public.testfam USING hash;
                               ^

 */

-- 聚集函数

CREATE FUNCTION mysfunc_accum(numeric, numeric, numeric)
    RETURNS numeric
AS 'select $1 + $2 + $3'
    LANGUAGE SQL
    RETURNS NULL ON NULL INPUT;

CREATE FUNCTION mycombine_accum(numeric, numeric )
    RETURNS numeric
AS 'select $1 + $2'
    LANGUAGE SQL
    RETURNS NULL ON NULL INPUT;

CREATE AGGREGATE agg_twocols(numeric, numeric) (
    SFUNC = mysfunc_accum,
    STYPE = numeric,
    COMBINEFUNC = mycombine_accum,
    INITCOND = 0 );

-- clean: DROP AGGREGATE agg_twocols(numeric, numeric); DROP FUNCTION mycombine_accum; DROP FUNCTION mysfunc_accum;

-- check
/*

db_test1=# \da agg_twocols;
                         List of aggregate functions
 Schema |    Name     | Result data type | Argument data types | Description
--------+-------------+------------------+---------------------+-------------
 public | agg_twocols | numeric          | numeric, numeric    |
(1 row)

gp5

WARNING:  aggregate attribute "combinefunc" not recognized

 */

-- 类型转换

CREATE CAST (text AS int) WITH INOUT;

-- clean: DROP CAST (text AS int);

-- check
/*

db_test1=# select count(*) from pg_cast where castsource=(select oid from pg_type where typname='text') and casttarget=(select oid from pg_type where typname='int4');
 count
-------
     1
(1 row)

gp5

ERROR:  syntax error at or near "INOUT"
LINE 12: CREATE CAST (text AS int) WITH INOUT;
                                        ^
 */

-- 访问方法

CREATE ACCESS METHOD test_access_method TYPE TABLE HANDLER heap_tableam_handler;

-- clean: DROP ACCESS METHOD test_access_method;

-- check
/*

db_test1=# \dA test_access_method
   List of access methods
        Name        | Type
--------------------+-------
 test_access_method | Table
(1 row)

GP6/PG9 does not have access method

db1=# CREATE ACCESS METHOD test_access_method TYPE TABLE HANDLER heap_tableam_handler;
ERROR:  syntax error at or near "ACCESS"
LINE 1: CREATE ACCESS METHOD test_access_method TYPE TABLE HANDLER h...
               ^
db1=#

(psql14)

db1=# \dA test_access_method
The server (version 9.4) does not support access methods.
db1=#

(psql9)
db1=# \dA test_access_method
Invalid command \dA. Try \? for help.


 */

-- 视图 (VIEW, MATERIALIZED VIEW)

CREATE VIEW v_vista AS SELECT 'Hello World';

-- clean: DROP VIEW v_vista;

-- check
/*

db_test1=# \dv v_vista
         List of relations
 Schema |  Name   | Type |  Owner
--------+---------+------+---------
 public | v_vista | view | gpadmin
(1 row)

gp6

WARNING:  column "?column?" has type "unknown"
DETAIL:  Proceeding with relation creation anyway.

 */

-- CREATE MATERIALIZED VIEW

CREATE TABLE tbl_films (id int, kind varchar);
CREATE MATERIALIZED VIEW v_comedies AS SELECT * FROM tbl_films WHERE kind = 'comedy';

-- clean: DROP MATERIALIZED VIEW v_comedies; DROP TABLE tbl_films;

-- check
/*

db_test1=# \dm v_comedies
                 List of relations
 Schema |    Name    |       Type        |  Owner
--------+------------+-------------------+---------
 public | v_comedies | materialized view | gpadmin
(1 row)

gp5

ERROR:  syntax error at or near "MATERIALIZED"
LINE 1: CREATE MATERIALIZED VIEW v_comedies AS SELECT * FROM tbl_fil...
               ^

 */

-- 序列

CREATE SEQUENCE myseq START 101;

-- clean: DROP SEQUENCE myseq;

-- check
/*

db_test1=# \ds myseq
          List of relations
 Schema | Name  |   Type   |  Owner
--------+-------+----------+---------
 public | myseq | sequence | gpadmin
(1 row)

 */

-- 约束 -- 域 (DOMAIN)

CREATE TABLE tbl_with_constraint (
    id     integer,
    name    varchar(40),
    CONSTRAINT con1 CHECK (id > 100 AND name <> '')
);

-- clean: DROP TABLE tbl_with_constraint;

-- check
/*

db_test1=# \dS tbl_with_constraint
               Table "public.tbl_with_constraint"
 Column |         Type          | Collation | Nullable | Default
--------+-----------------------+-----------+----------+---------
 id     | integer               |           |          |
 name   | character varying(40) |           |          |
Check constraints:
    "con1" CHECK (id > 100 AND name::text <> ''::text)
Distributed by: (id)

(pg14 - 3x)
\dS tbl_with_constraint
psql:resources/all_types_objects.check.sql:33: ERROR:  column "attrnums" does not exist
LINE 1: SELECT attrnums
               ^
               Table "public.tbl_with_constraint"
 Column |         Type          | Collation | Nullable | Default
--------+-----------------------+-----------+----------+---------
 id     | integer               |           |          |
 name   | character varying(40) |           |          |
Check constraints:
    "con1" CHECK (id > 100 AND name::text <> ''::text)
Tablespace: "dfs_tablespace"

 */

-- CREATE DOMAIN

CREATE DOMAIN us_postal_code_domain AS TEXT
    CHECK(
            VALUE ~ '^\d{5}$'
            OR VALUE ~ '^\d{5}-\d{4}$'
        );

-- clean: DROP DOMAIN us_postal_code_domain;

-- check
/*

db_test1=# \dD us_postal_code_domain
                                                              List of domains
 Schema |         Name          | Type | Collation | Nullable | Default |                              Check
--------+-----------------------+------+-----------+----------+---------+------------------------------------------------------------------
 public | us_postal_code_domain | text |           |          |         | CHECK (VALUE ~ '^\d{5}$'::text OR VALUE ~ '^\d{5}-\d{4}$'::text)
(1 row)

 */

-- 字符集转换

CREATE CONVERSION public.testconv FOR 'LATIN1' TO 'MULE_INTERNAL' FROM latin1_to_mic;

-- clean: DROP CONVERSION public.testconv;

-- check
/*

db_test1=# \dc testconv
                  List of conversions
 Schema |   Name   | Source |  Destination  | Default?
--------+----------+--------+---------------+----------
 public | testconv | LATIN1 | MULE_INTERNAL | no
(1 row)

 */

-- 索引

CREATE TABLE books (author varchar, title varchar, total int);
CREATE UNIQUE INDEX title_idx ON books (author, title);

-- clean: DROP TABLE books;

-- check
/*

db_test1=# \dS books
                    Table "public.books"
 Column |       Type        | Collation | Nullable | Default
--------+-------------------+-----------+----------+---------
 author | character varying |           |          |
 title  | character varying |           |          |
 total  | integer           |           |          |
Indexes:
    "title_idx" UNIQUE, btree (author, title)
Distributed by: (author)

(pg14 on 3x)

\dS books
psql:resources/all_types_objects.check.sql:35: ERROR:  column "attrnums" does not exist
LINE 1: SELECT attrnums
               ^
                    Table "public.books"
 Column |       Type        | Collation | Nullable | Default
--------+-------------------+-----------+----------+---------
 author | character varying |           |          |
 title  | character varying |           |          |
 total  | integer           |           |          |
Tablespace: "dfs_tablespace"

 */

-- 规则

CREATE TABLE t1_for_rule (c1 int);
CREATE TABLE t2_for_rule (c1 int);

CREATE RULE rule_abc AS
    ON INSERT TO t1_for_rule
    DO INSERT INTO public.t2_for_rule VALUES (1);

-- clean: DROP RULE rule_abc ON t1_for_rule; DROP TABLE t1_for_rule; DROP TABLE t2_for_rule;

-- check
/*

db_test1=# select count(*) from pg_rules where rulename='rule_abc';
 count
-------
     1
(1 row)

 */

-- 触发器

CREATE TABLE testtable (c1 int, c2 varchar);
CREATE TRIGGER testtable_trigger AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH ROW EXECUTE FUNCTION "RI_FKey_check_ins"();

CREATE OR REPLACE FUNCTION public.postdata_eventtrigger_func() RETURNS event_trigger AS $$ BEGIN END $$ LANGUAGE plpgsql;
CREATE EVENT TRIGGER postdata_eventtrigger ON sql_drop EXECUTE PROCEDURE public.postdata_eventtrigger_func();

-- clean: DROP EVENT TRIGGER postdata_eventtrigger; DROP FUNCTION public.postdata_eventtrigger_func; DROP TRIGGER testtable_trigger ON public.testtable; DROP TABLE testtable;

-- check
/*

db_test1=# select count(*) from pg_trigger where tgname='testtable_trigger';
 count
-------
     1
(1 row)

db_test1=# \dy postdata_eventtrigger
                                  List of event triggers
         Name          |  Event   |  Owner  | Enabled |          Function          | Tags
-----------------------+----------+---------+---------+----------------------------+------
 postdata_eventtrigger | sql_drop | gpadmin | enabled | postdata_eventtrigger_func |
(1 row)

3x, seems not support trigger

db1=# CREATE TRIGGER testtable_trigger AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH ROW EXECUTE PROCEDURE "RI_FKey_check_ins"();
ERROR:  ON UPDATE triggers are not supported on append-only tables

db1=# select count(*) from pg_trigger where tgname='testtable_trigger';
 count
-------
     0
(1 row)

gp5

db1=# CREATE TRIGGER testtable_trigger AFTER INSERT OR DELETE OR UPDATE ON public.testtable FOR EACH ROW EXECUTE FUNCTION "RI_FKey_check_ins"();
ERROR:  syntax error at or near "FUNCTION"
LINE 1: ...R UPDATE ON public.testtable FOR EACH ROW EXECUTE FUNCTION "...
                                                             ^

db1=# CREATE OR REPLACE FUNCTION public.postdata_eventtrigger_func() RETURNS event_trigger AS $$ BEGIN END $$ LANGUAGE plpgsql;
ERROR:  type "event_trigger" does not exist

db1=# CREATE EVENT TRIGGER postdata_eventtrigger ON sql_drop EXECUTE PROCEDURE public.postdata_eventtrigger_func();
ERROR:  syntax error at or near "EVENT"
LINE 1: CREATE EVENT TRIGGER postdata_eventtrigger ON sql_drop EXECU...
               ^


 */

-- 权限 (ROLE, PRIVILEGES, GROUP, USER)

-- CREATE ROLE, ALTER PRIVILEGES

CREATE ROLE jonathan LOGIN;

-- clean: DROP ROLE jonathan;

-- check

/*

db_test1=# \dg jonathan
           List of roles
 Role name | Attributes | Member of
-----------+------------+-----------
 jonathan  |            | {}

 */

-- CREATE GROUP

CREATE GROUP group1;

-- clean: DROP GROUP group1;

-- check

/*

db_test1=# \dg group1
            List of roles
 Role name |  Attributes  | Member of
-----------+--------------+-----------
 group1    | Cannot login | {}

 */

-- CREATE USER

CREATE USER user1;

-- clean: DROP USER user1;

-- check
/*

db_test1=# \dg user1
           List of roles
 Role name | Attributes | Member of
-----------+------------+-----------
 user1     |            | {}

 */

-- 行安全策略

-- CREATE POLICY

CREATE TABLE accounts (manager text, company text, contact_email text);

ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;

CREATE POLICY account_policy ON accounts USING (manager = current_user);

-- clean: DROP POLICY account_policy ON accounts; DROP TABLE accounts;

-- check
/*

db_test1=# select count(*) from pg_policy where polname='account_policy';
 count
-------
     1
(1 row)

GP6/PG9 does not have create policy

db1=# CREATE POLICY account_policy ON accounts USING (manager = current_user);
ERROR:  syntax error at or near "POLICY"
LINE 1: CREATE POLICY account_policy ON accounts USING (manager = cu...
               ^
db1=# select count(*) from pg_policy where polname='account_policy';
ERROR:  relation "pg_policy" does not exist
LINE 1: select count(*) from pg_policy where polname='account_policy...
                             ^

 */

-- 资源组/队列 (RESOURCE GROUP, RESOURCE QUEUE)

-- CREATE RESOURCE GROUP

CREATE RESOURCE GROUP rgroup1 WITH (CPU_RATE_LIMIT=35, MEMORY_LIMIT=35);

-- clean: DROP RESOURCE GROUP rgroup1;

-- check
/*

db_test1=# select count(*) from pg_resgroup where rsgname='rgroup1';
 count
-------
     1
(1 row)

gp4

postgres=# CREATE RESOURCE GROUP rgroup1 WITH (CPU_RATE_LIMIT=35, MEMORY_LIMIT=35);
ERROR:  syntax error at or near "GROUP"
LINE 1: CREATE RESOURCE GROUP rgroup1 WITH (CPU_RATE_LIMIT=35, MEMOR...
                        ^
postgres=# CREATE RESOURCE QUEUE myqueue WITH (ACTIVE_STATEMENTS=20);
CREATE QUEUE

 */

-- CREATE RESOURCE QUEUE

CREATE RESOURCE QUEUE myqueue WITH (ACTIVE_STATEMENTS=20);

-- clean: DROP RESOURCE QUEUE myqueue;

-- check
/*

db_test1=# select count(*) from pg_resqueue where rsqname='myqueue';
 count
-------
     1
(1 row)

 */

-- 扩展统计信息

CREATE TABLE s_t1 (a int, b int);

CREATE STATISTICS s1 (dependencies) ON a, b FROM s_t1;

-- clean: DROP STATISTICS s1; DROP TABLE s_t1;

-- check
/*

db1=# \dX s1
                   List of extended statistics
 Schema | Name |   Definition   | Ndistinct | Dependencies | MCV
--------+------+----------------+-----------+--------------+-----
 public | s1   | a, b FROM s_t1 |           | defined      |
(1 row)

GP6/PG9 does not have CREATE STATISTICS

db1=# CREATE STATISTICS s1 (dependencies) ON a, b FROM s_t1;
ERROR:  syntax error at or near "STATISTICS"
LINE 1: CREATE STATISTICS s1 (dependencies) ON a, b FROM s_t1;
               ^
(pg14)
db1=# \dX s1
The server (version 9.4) does not support extended statistics.

(pg9)
db1=# \dX s1
Invalid command \dX. Try \? for help.

db1=# select * from pg_statistic_ext where stxname='s1';
ERROR:  relation "pg_statistic_ext" does not exist
LINE 1: select * from pg_statistic_ext where stxname='s1';

 */


-----------------------------------

-----------------------------------