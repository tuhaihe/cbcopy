CREATE SCHEMA schema1;
CREATE SCHEMA schema2;

CREATE TABLE t1 (c1 int, c2 varchar);
CREATE TABLE schema1.t1 (c1 int, c2 varchar);
CREATE TABLE schema2.t21 (c1 int, c2 varchar);
CREATE TABLE schema2.t22 (c1 int, c2 varchar);

INSERT INTO t1 VALUES (1, 'public'),(2, 'public'),(3, 'public');
INSERT INTO schema1.t1 VALUES (1, 'schema1'),(2, 'schema1'),(3, 'schema1');
INSERT INTO schema2.t21 VALUES (1, 'schema2-t21'),(2, 'schema2-t21'),(3, 'schema2-t21');
INSERT INTO schema2.t22 VALUES (1, 'schema2-t22'),(2, 'schema2-t22'),(3, 'schema2-t22');
