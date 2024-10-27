\dn schema*
select * from t1 order by c1; select * from schema1.t1 order by c1; select * from schema2.t1 order by c1;
--\dE ext_expenses;
select relname, urilocation from pg_exttable join pg_class on pg_exttable.reloid=pg_class.oid and pg_class.relname='ext_expenses';
\dE ft1
\des sc
\deu+
\db mytblspace
\df add
\df insert_data
select count(*) from pg_extprotocol where ptcname='p1';
\dx hstore
\dO collation_french
\dT type_mood
\dL plperlabc
select trffromsql, trftosql from pg_transform order by trffromsql, trftosql;
\dF testconfiguration
\dFd testdictionary
\dFp testparser
\dFt testtemplate
\dAc gist integer;
\dAf gist integer
\dAo gist testclass
\dAp gist testclass
select count(*) from pg_opfamily where opfname='testfam';
\da agg_twocols;
select count(*) from pg_cast where castsource=(select oid from pg_type where typname='text') and casttarget=(select oid from pg_type where typname='int4');
--\dA test_access_method
select count(*) from pg_am where amname='test_access_method';
\dv v_vista
\dm v_comedies
\ds myseq
--\dS tbl_with_constraint
select count(*) from tbl_with_constraint;
\dD us_postal_code_domain
\dc testconv
--\dS books
select count(*) from books;
select count(*) from pg_rules where rulename='rule_abc';
select count(*) from pg_trigger where tgname='testtable_trigger';
\dy postdata_eventtrigger
\dg jonathan
\dg group1
\dg user1
select count(*) from pg_policy where polname='account_policy';
select count(*) from pg_resgroup where rsgname='rgroup1';
select count(*) from pg_resqueue where rsqname='myqueue';
--\dX s1
select count(*) from pg_statistic_ext where stxname='s1';
