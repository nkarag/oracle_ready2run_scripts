drop table TSMALL

create table TSMALL as
  select level id, rpad('x',DBMS_RANDOM.random,'x') descr
  from dual
  connect by level < 100
  
exec dbms_stats.gather_table_stats('NIKOS', 'TSMALL')  

drop table TLARGE  
  
create table TLARGE as select rownum id, rpad('x',DBMS_RANDOM.random,'x') descr
  from dual
  connect by level < 10000
  
exec dbms_stats.gather_table_stats('NIKOS', 'TLARGE')

-- create local db link

CREATE PUBLIC DATABASE LINK "loopback" connect to nikos identified by "nikos"
USING 'localhost:1521/nikosdb';


-- create dblink at nikosdb pointing to exadata prod
create public database link exadwhprd connect to nkarag identified by "KIXem!123" using 'exadwhprd';

-- create local tsmall table
create table TSMALL_local as
    select level id, rpad('x',DBMS_RANDOM.random,'x') descr
    from dual
    connect by level < 100


-- create a distributed query
select *
from tsmall_local lcl, tlarge@exadwhprd rmt
where
	lcl.id = rmt.id
	
-- (enallaktika)
select *
from tsmall_local lcl, tlarge@loopback rmt
where
	lcl.id = rmt.id	

Elapsed: 00:00:00.41	
	-- για κάθε γραμμή του Local πίνακα κάνει access το remote Πίνακα
	
Plan hash value: 2127595353

----------------------------------------------------------------------------------------------------
| Id  | Operation          | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|
----------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |              |        |       |   920 (100)|          |        |      |
|   1 |  NESTED LOOPS      |              |     99 |   389K|   920   (2)| 00:00:12 |        |      |
|   2 |   TABLE ACCESS FULL| TSMALL_LOCAL |     99 |   194K|    16   (0)| 00:00:01 |        |      |
|   3 |   REMOTE           | TLARGE       |      1 |  2015 |     9   (0)| 00:00:01 | EXADW~ | R->S |
----------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   2 - SEL$1 / LCL@SEL$1
   3 - SEL$1 / RMT@SEL$1

Remote SQL Information (identified by operation id):
----------------------------------------------------

   3 - SELECT "ID","DESCR" FROM "TLARGE" "RMT" WHERE :1="ID" (accessing 'EXADWHPRD' )
	
	
select /*+ leading(lcl) use_hash(rmt)  */ *
from tsmall_local lcl, tlarge@exadwhprd rmt
where
	lcl.id = rmt.id

Elapsed: 00:00:04.30

	-- αν κάνουμε hash_join τότε φέρνει τον μεγάλο πίνακα από το remote site τοπικά (κουβαλάει πολλά data στο δίκτυο)

PLAN_TABLE_OUTPUT
---------------------------------------------------------------------------------------------------------------------------------
SQL_ID  5wah46fr0c2rn, child number 0
-------------------------------------
select /*+ leading(lcl) use_hash(rmt)  */ * from tsmall_local lcl,
tlarge@exadwhprd rmt where  lcl.id = rmt.id

Plan hash value: 1246216434

-------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|  OMem |  1Mem | Used-Mem |
-------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |              |        |       | 29296 (100)|          |        |      |    |          |          |
|*  1 |  HASH JOIN         |              |     99 |   389K| 29296   (1)| 00:05:52 |        |      |   732K|   732K| 1226K (0)|
|   2 |   TABLE ACCESS FULL| TSMALL_LOCAL |     99 |   194K|    16   (0)| 00:00:01 |        |      |    |          |          |
|   3 |   REMOTE           | TLARGE       |  10000 |    19M| 29279   (1)| 00:05:52 | EXADW~ | R->S |    |          |          |
-------------------------------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   2 - SEL$1 / LCL@SEL$1
   3 - SEL$1 / RMT@SEL$1

Predicate Information (identified by operation id):
---------------------------------------------------

   1 - access("LCL"."ID"="RMT"."ID")

Remote SQL Information (identified by operation id):
----------------------------------------------------

   3 - SELECT /*+ USE_HASH ("RMT") */ "ID","DESCR" FROM "TLARGE" "RMT" (accessing 'EXADWHPRD' )
	
	
select /*+ driving_site(rmt)   */ *
from tsmall_local lcl, tlarge@exadwhprd rmt
where
	lcl.id = rmt.id	

Elapsed: 00:00:03.31

	-- Δες το note "fully remote statement"
	-- Δεν υπάρχει πλάνο ούτε στην v$sql_plan. To παρακάτω είναι από explain plan 
	-- Ο μικρός πίνακας στέλνεται στο remote site και επιστρέφουν πίσω τα αποτελέσματα από το join

PLAN_TABLE_OUTPUT
-----------------------------------------------------------------------------------------------------------------------------
Plan hash value: 186187723

-----------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                       | Name         | Rows  | Bytes | Cost (%CPU)| Time     | TQ/Ins |IN-OUT| PQ Distrib |
-----------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT REMOTE         |              |  4411 |    17M|  9055   (1)| 00:00:01 |     |      |       |
|   1 |  PX COORDINATOR                 |              |       |       |            |          |     |      |       |
|   2 |   PX SEND QC (RANDOM)           | :TQ10002     |  4411 |    17M|  9055   (1)| 00:00:01 |  Q1,02 | P->S | QC (RAND)  |
|*  3 |    HASH JOIN BUFFERED           |              |  4411 |    17M|  9055   (1)| 00:00:01 |  Q1,02 | PCWP |            |
|   4 |     BUFFER SORT                 |              |       |       |            |          |  Q1,02 | PCWC |            |
|   5 |      PX RECEIVE                 |              |  4411 |  8679K|    11   (0)| 00:00:01 |  Q1,02 | PCWP |            |
|   6 |       PX SEND HASH              | :TQ10000     |  4411 |  8679K|    11   (0)| 00:00:01 | DWHPRD | S->P | HASH       |
|   7 |        REMOTE                   | TSMALL_LOCAL |  4411 |  8679K|    11   (0)| 00:00:01 |      ! | R->S |            |
|   8 |     PX RECEIVE                  |              | 10000 |    19M|  9043   (0)| 00:00:01 |  Q1,02 | PCWP |            |
|   9 |      PX SEND HASH               | :TQ10001     | 10000 |    19M|  9043   (0)| 00:00:01 |  Q1,01 | P->P | HASH       |
|  10 |       PX BLOCK ITERATOR         |              | 10000 |    19M|  9043   (0)| 00:00:01 |  Q1,01 | PCWC |            |
|  11 |        TABLE ACCESS STORAGE FULL| TLARGE       | 10000 |    19M|  9043   (0)| 00:00:01 |  Q1,01 | PCWP |            |
-----------------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

   3 - access("A2"."ID"="A1"."ID")

Remote SQL Information (identified by operation id):
----------------------------------------------------

   7 - SELECT "ID","DESCR" FROM "TSMALL_LOCAL" "A2" (accessing '!' )


Note
-----
   - fully remote statement

33 rows selected.

select /*+ driving_site(rmt) noparallel  */ *
from tsmall_local lcl, tlarge@exadwhprd rmt
where
	lcl.id = rmt.id	



-- δοκιμάζω με το loopback
select /*+ driving_site(rmt)  */ *
from tsmall_local lcl, tlarge@loopback rmt
where
	lcl.id = rmt.id	
	
	-- εκτέλεσε το query στο remote site αλλά επέλεξε NESTED LOOPS.
	-- Επομένως, το query (δηλ. το NESTED LOOPS) εκτελείται στο remote site και τα data από το driving table (tsmall_local)
	-- έρχονται από το Local site.

nikos@NIKOSDB> select * from table(dbms_xplan.display)
  2  /

PLAN_TABLE_OUTPUT
---------------------------------------------------------------------------------------------------------------
Plan hash value: 2565364479

-------------------------------------------------------------------------------------------------------------
| Id  | Operation                    | Name         | Rows  | Bytes | Cost (%CPU)| Time     | Inst   |IN-OUT|
-------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT REMOTE      |              |    99 |   381K|   209   (0)| 00:00:03 |        |      |
|   1 |  NESTED LOOPS                |              |       |       |            |          |        |      |
|   2 |   NESTED LOOPS               |              |    99 |   381K|   209   (0)| 00:00:03 |        |      |
|   3 |    REMOTE                    | TSMALL_LOCAL |    99 |   187K|    11   (0)| 00:00:01 |      ! | R->S |
|*  4 |    INDEX RANGE SCAN          | TLARGE_IDX   |     1 |       |     1   (0)| 00:00:01 | NIKOS~ |      |
|   5 |   TABLE ACCESS BY INDEX ROWID| TLARGE       |     1 |  2006 |     2   (0)| 00:00:01 | NIKOS~ |      |
-------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

   4 - access("A2"."ID"="A1"."ID")

Remote SQL Information (identified by operation id):
----------------------------------------------------

   3 - SELECT "ID","DESCR" FROM "TSMALL_LOCAL" "A2" (accessing '!' )


Note
-----
   - fully remote statement	


-- Ο J Lewis λέει ότι το driving_site δεν παίζει με CTAS και INSERT INTO SELECT
-- για να το δούμε ...

create table testrmt2
as select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
from tsmall_local lcl, tlarge@exadwhprd rmt
where
	lcl.id = rmt.id	

	-- πράγματι το αγνοεί to driving_site
PLAN_TABLE_OUTPUT
-----------------------------------------------------------------------------------------------------------------------------------
SQL_ID  2unn0zh4c88uk, child number 0
-------------------------------------
create table testrmt2 as select /*+ driving_site(rmt)  */ lcl.id,
rmt.descr from tsmall_local lcl, tlarge@exadwhprd rmt where  lcl.id =
rmt.id

Plan hash value: 2479874892

-----------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation              | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------
|   0 | CREATE TABLE STATEMENT |              |        |       |   927 (100)|          |        |      |       |       |          |
|   1 |  LOAD AS SELECT        |              |        |       |            |          |        |      |   521K|   521K|  521K (0)|
|   2 |   NESTED LOOPS         |              |     99 |   196K|   920   (2)| 00:00:12 |        |      |       |       |          |
|   3 |    TABLE ACCESS FULL   | TSMALL_LOCAL |     99 |  1287 |    16   (0)| 00:00:01 |        |      |       |       |          |
|   4 |    REMOTE              | TLARGE       |      1 |  2015 |     9   (0)| 00:00:01 | EXADW~ | R->S |       |       |          |
-----------------------------------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   3 - SEL$1 / LCL@SEL$1
   4 - SEL$1 / RMT@SEL$1

Remote SQL Information (identified by operation id):
----------------------------------------------------

   4 - SELECT /*+ */ "ID","DESCR" FROM "TLARGE" "RMT" WHERE :1="ID" (accessing 'EXADWHPRD' )


create table testrmt3
as select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
from tsmall_local lcl, tlarge@loopback rmt
where
	lcl.id = rmt.id	

Plan hash value: 2479874892

-----------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation              | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------
|   0 | CREATE TABLE STATEMENT |              |        |       |   233 (100)|          |        |      |       |       |          |
|   1 |  LOAD AS SELECT        |              |        |       |            |          |        |      |   521K|   521K|  521K (0)|
|   2 |   NESTED LOOPS         |              |     99 |   194K|   226   (6)| 00:00:03 |        |      |       |       |          |
|   3 |    TABLE ACCESS FULL   | TSMALL_LOCAL |     99 |   297 |    16   (0)| 00:00:01 |        |      |       |       |          |
|   4 |    REMOTE              | TLARGE       |      1 |  2006 |     2   (0)| 00:00:01 | LOOPB~ | R->S |       |       |          |
-----------------------------------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   3 - SEL$1 / LCL@SEL$1
   4 - SEL$1 / RMT@SEL$1

Remote SQL Information (identified by operation id):
----------------------------------------------------

   4 - SELECT /*+ */ "ID","DESCR" FROM "TLARGE" "RMT" WHERE :1="ID" (accessing 'LOOPBACK' )

	
-- ας δούμε και το INSERT

nikos@NIKOSDB> create table testrmt as select * from tsmall_local where 1=0
  2  /

Table created.

insert into testrmt
select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
from tsmall_local lcl, tlarge@exadwhprd rmt
where
	lcl.id = rmt.id	

	-- πράγματι, το αγνοεί το driving_Site
	
PLAN_TABLE_OUTPUT
------------------------------------------------------------------------------------------------------------
SQL_ID  0g6cwhxvfwhm2, child number 0
-------------------------------------
insert into testrmt select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
from tsmall_local lcl, tlarge@exadwhprd rmt where  lcl.id = rmt.id

Plan hash value: 2127595353

----------------------------------------------------------------------------------------------------------
| Id  | Operation                | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|
----------------------------------------------------------------------------------------------------------
|   0 | INSERT STATEMENT         |              |        |       |   920 (100)|          |        |   |
|   1 |  LOAD TABLE CONVENTIONAL |              |        |       |            |          |        |   |
|   2 |   NESTED LOOPS           |              |     99 |   196K|   920   (2)| 00:00:12 |        |   |
|   3 |    TABLE ACCESS FULL     | TSMALL_LOCAL |     99 |  1287 |    16   (0)| 00:00:01 |        |   |
|   4 |    REMOTE                | TLARGE       |      1 |  2015 |     9   (0)| 00:00:01 | EXADW~ | R->S |
----------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   3 - SEL$1 / LCL@SEL$1
   4 - SEL$1 / RMT@SEL$1

Remote SQL Information (identified by operation id):
----------------------------------------------------

   4 - SELECT /*+ OPAQUE_TRANSFORM */ "ID","DESCR" FROM "TLARGE" "RMT" WHERE :1="ID" (accessing
       'EXADWHPRD' )
	   
	   
		-- Μπορούμε να το κρύψουμε πίσω από ένα subquery?
insert into testrmt
with q as (
	select /*+ driving_site(rmt) no_merge  */ lcl.id, rmt.descr
	from tsmall_local lcl, tlarge@exadwhprd rmt
	where
		lcl.id = rmt.id	
)	   
select * from q;


	-- Oxi
	PLAN_TABLE_OUTPUT
-------------------------------------------------------------------------------------------------------------
SQL_ID  f615mfzj00mxj, child number 0
-------------------------------------
insert into testrmt with q as (  select /*+ driving_site(rmt) no_merge
*/ lcl.id, rmt.descr  from tsmall_local lcl, tlarge@exadwhprd rmt
where   lcl.id = rmt.id ) select * from q

Plan hash value: 2284146299

----------------------------------------------------------------------------------------------------------
| Id  | Operation                | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|
----------------------------------------------------------------------------------------------------------
|   0 | INSERT STATEMENT         |              |        |       |   920 (100)|          |        |  	 |
|   1 |  LOAD TABLE CONVENTIONAL |              |        |       |            |          |        |   	 |
|   2 |   VIEW                   |              |     99 |   194K|   920   (2)| 00:00:12 |        |      |
|   3 |    NESTED LOOPS          |              |     99 |   196K|   920   (2)| 00:00:12 |        |      |
|   4 |     TABLE ACCESS FULL    | TSMALL_LOCAL |     99 |  1287 |    16   (0)| 00:00:01 |        |      |
|   5 |     REMOTE               | TLARGE       |      1 |  2015 |     9   (0)| 00:00:01 | EXADW~ | R->S |
----------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$2
   2 - SEL$1 / Q@SEL$2
   3 - SEL$1
   4 - SEL$1 / LCL@SEL$1
   5 - SEL$1 / RMT@SEL$1

Remote SQL Information (identified by operation id):
----------------------------------------------------

   5 - SELECT /*+ */ "ID","DESCR" FROM "TLARGE" "RMT" WHERE :1="ID" (accessing 'EXADWHPRD' )

   
   -- αν φτιάξουμε ένα view;
   
   create or replace view v_testrmt as
   select /*+ driving_site(rmt) no_merge  */ lcl.id, rmt.descr
	from tsmall_local lcl, tlarge@exadwhprd rmt
	where
		lcl.id = rmt.id;

insert into testrmt
select * from 	v_testrmt	

	-- tzifos!
	
	PLAN_TABLE_OUTPUT
--------------------------------------------------------------------------------------------------------------
SQL_ID  4s5wffxwgfkb1, child number 0
-------------------------------------
insert into testrmt select * from  v_testrmt

Plan hash value: 3446095474

----------------------------------------------------------------------------------------------------------
| Id  | Operation                | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|
----------------------------------------------------------------------------------------------------------
|   0 | INSERT STATEMENT         |              |        |       |   920 (100)|          |        |   |
|   1 |  LOAD TABLE CONVENTIONAL |              |        |       |            |          |        |   |
|   2 |   VIEW                   | V_TESTRMT    |     99 |   194K|   920   (2)| 00:00:12 |        |   |
|   3 |    NESTED LOOPS          |              |     99 |   196K|   920   (2)| 00:00:12 |        |   |
|   4 |     TABLE ACCESS FULL    | TSMALL_LOCAL |     99 |  1287 |    16   (0)| 00:00:01 |        |   |
|   5 |     REMOTE               | TLARGE       |      1 |  2015 |     9   (0)| 00:00:01 | EXADW~ | R->S |
----------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   2 - SEL$2 / V_TESTRMT@SEL$1
   3 - SEL$2
   4 - SEL$2 / LCL@SEL$2
   5 - SEL$2 / RMT@SEL$2

Remote SQL Information (identified by operation id):
----------------------------------------------------

   5 - SELECT /*+ */ "ID","DESCR" FROM "TLARGE" "RMT" WHERE :1="ID" (accessing 'EXADWHPRD' )


 -- O J Lewis προτείνει σαν workaround:
 /*
 "here is a special warning that goes with this hint – it isn’t valid for the select statements in “create as select” 
 and “insert as select”. There seems to be no good reason for this limitation, but for CTAS and “insert as select” 
 the query has to operate at the site of the table that is receiving the data. This means that you may be able 
 to tune a naked SELECT to perform very well and then find that you can’t get the CTAS to use the same execution plan. 
 A typical workaround to this problem is to wrap the select statement into a pipelined function 
 and do a select from table(pipelined_function)."
 */

-- See: "Performing Multiple Transformations with Pipelined Table Functions" 
-- 		Oracle® Database PL/SQL Language Reference 11g Release 2 (11.2)
 
-- the select statement we want to wrap in a pipelined table function is the following:
select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
from tsmall_local lcl, tlarge@exadwhprd rmt
where
	lcl.id = rmt.id

-- so the distributed insert	 will become
insert into testrmt
select *
from	table(
	remdml_pkg.remsel(  CURSOR (	select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
									from tsmall_local lcl, tlarge@exadwhprd rmt
									where
									lcl.id = rmt.id
								) 
					)
		);

 
CREATE OR REPLACE PACKAGE remdml_pkg IS
  TYPE refcur_t IS REF CURSOR RETURN tsmall_local%ROWTYPE;
  TYPE outrec_typ IS RECORD (
    id    NUMBER(22),
    descr  VARCHAR2(4000)
  );
  TYPE outrecset IS TABLE OF outrec_typ;
  FUNCTION remsel (p refcur_t) RETURN outrecset PIPELINED;
END remdml_pkg;
/

CREATE OR REPLACE PACKAGE BODY remdml_pkg IS
  FUNCTION remsel (p refcur_t) RETURN outrecset PIPELINED IS
    out_rec outrec_typ;
    in_rec  p%ROWTYPE;
  BEGIN
    LOOP
      FETCH p INTO in_rec;
      EXIT WHEN p%NOTFOUND;
      -- first row
      out_rec.id := in_rec.id;
      out_rec.descr := in_rec.descr;
      PIPE ROW(out_rec);
    END LOOP;
    CLOSE p;
    RETURN;
  END remsel;
END remdml_pkg;
 
-- test query
select *
from	table(
	remdml_pkg.remsel(  CURSOR (	select id, descr
									from tsmall_local lcl
								) 
					)
		); 

   ID DESCR
----- --------------------
    1
    2 xxxxxxxxxxxxxxxxxxxx
    3
    4
    5
    6 xxxxxxxxxxxxxxxxxxxx
    7
    8 xxxxxxxxxxxxxxxxxxxx
    9
   10
...   
99 rows selected.

select *
from	table(
	remdml_pkg.remsel(  CURSOR (	select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
									from tsmall_local lcl, tlarge@loopback rmt
									where
									lcl.id = rmt.id
								) 
					)
		);		
		
		-- you CANT SEE what going on inside the table function!
		-- in order ot verify that the join executed at the remote site...
 
PLAN_TABLE_OUTPUT
------------------------------------------------------------------------------------------------
SQL_ID  35hgsr97735ud, child number 1
-------------------------------------
select * from table(  remdml_pkg.remsel(  CURSOR ( select /*+
driving_site(rmt)  */ lcl.id, rmt.descr          from tsmall_local lcl,
tlarge@loopback rmt          where          lcl.id = rmt.id         )
   )   )

Plan hash value: 1024074117

----------------------------------------------------------------------------------------------
| Id  | Operation                          | Name   | E-Rows |E-Bytes| Cost (%CPU)| E-Time   |
----------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                   |        |        |       |    29 (100)|          |
|   1 |  VIEW                              |        |    198 |  4356 |    29   (0)| 00:00:01 |
|   2 |   COLLECTION ITERATOR PICKLER FETCH| REMSEL |    198 |       |    29   (0)| 00:00:01 |
----------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$E112F6F0 / from$_subquery$_001@SEL$1
   2 - SEL$E112F6F0 / KOKBF$@SEL$E112F6F0

Note
-----
   - cardinality feedback used for this statement
   - Warning: basic plan statistics not available. These are only collected when:
       * hint 'gather_plan_statistics' is used for the statement or
       * parameter 'statistics_level' is set to 'ALL', at session or system level
 
 
-- Similarly for the insert

insert into testrmt
select *
from	table(
	remdml_pkg.remsel(  CURSOR (	select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
									from tsmall_local lcl, tlarge@exadwhprd rmt
									where
									lcl.id = rmt.id
								) 
					)
		);

97 rows created.

Elapsed: 00:00:00.16		

PLAN_TABLE_OUTPUT
-----------------------------------------------------------------------------------------------
SQL_ID  9cqm2ssd8w0cu, child number 0
-------------------------------------
insert into testrmt select * from table(  remdml_pkg.remsel(  CURSOR (
select /*+ driving_site(rmt)  */ lcl.id, rmt.descr          from
tsmall_local lcl, tlarge@loopback rmt          where          lcl.id =
rmt.id         )      )   )

Plan hash value: 1024074117

-----------------------------------------------------------------------------------------------
| Id  | Operation                           | Name   | E-Rows |E-Bytes| Cost (%CPU)| E-Time   |
-----------------------------------------------------------------------------------------------
|   0 | INSERT STATEMENT                    |        |        |       |    29 (100)|          |
|   1 |  LOAD TABLE CONVENTIONAL            |        |        |       |            |          |
|   2 |   VIEW                              |        |   8168 |   175K|    29   (0)| 00:00:01 |
|   3 |    COLLECTION ITERATOR PICKLER FETCH| REMSEL |   8168 |       |    29   (0)| 00:00:01 |
-----------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   2 - SEL$E112F6F0 / from$_subquery$_002@SEL$1
   3 - SEL$E112F6F0 / KOKBF$@SEL$E112F6F0

Note
-----
   - Warning: basic plan statistics not available. These are only collected when:
       * hint 'gather_plan_statistics' is used for the statement or
       * parameter 'statistics_level' is set to 'ALL', at session or system level


31 rows selected.

Elapsed: 00:00:00.09

-- να δούμε πόσο χρόνο κάνει χωρίς το table function
	insert into testrmt
    select /*+ driving_site(rmt)  */ lcl.id, rmt.descr
    from tsmall_local lcl, tlarge@exadwhprd rmt
    where
       lcl.id = rmt.id
    /

97 rows created.

Elapsed: 00:00:00.73

-- Check case where a predicate is NOT send to the remote site because of a function in the predicate
-- (case from my mail)

select *
from tsmall_local lcl, tlarge@loopback rmt
where
	2*lcl.id + 1 = rmt.id
	
	-- DEN douleyei me expression
	
Plan hash value: 1246216434

-------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name         | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT|  OMem |  1Mem | Used-Mem |
-------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |              |        |       |   920 (100)|          |        |      |    |          |          |
|*  1 |  HASH JOIN         |              |   4171 |    16M|   920   (1)| 00:00:12 |        |      |   732K|   732K| 1244K (0)|
|   2 |   TABLE ACCESS FULL| TSMALL_LOCAL |     99 |   195K|    17   (0)| 00:00:01 |        |      |    |          |          |
|   3 |   REMOTE           | TLARGE       |    421K|   809M|   900   (1)| 00:00:11 | LOOPB~ | R->S |    |          |          |
-------------------------------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   2 - SEL$1 / LCL@SEL$1
   3 - SEL$1 / RMT@SEL$1

Predicate Information (identified by operation id):
---------------------------------------------------

   1 - access("RMT"."ID"=2*"LCL"."ID"+1)

Remote SQL Information (identified by operation id):
----------------------------------------------------

   3 - SELECT "ID","DESCR" FROM "TLARGE" "RMT" (accessing 'LOOPBACK' )	



		-- AS DOKIMASOUME ME HINT GIA NL, KAI ME FUNCTION
select /*+ leading(lcl) use_nl(rmt) */ *
from tsmall_local lcl, tlarge@loopback rmt
where
	power(lcl.id,2) = rmt.id
	
	-- OUTE TWRA DOULEPSE. STELNEI TO PREDICATE KANONIKA STO REMOTE SITE
	
SQL_ID  f2abs3mxpxw7v, child number 0
-------------------------------------
select /*+ leading(lcl) use_nl(rmt) */ * from tsmall_local lcl,
tlarge@loopback rmt where  power(lcl.id,2) = rmt.id

Plan hash value: 2127595353

---------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name         | Starts | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT| A-Rows |   A-Time   | Buffers |
---------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |              |      1 |        |       | 89666 (100)|          |        |      |     99 |00:00:00.59 |      56 |
|   1 |  NESTED LOOPS      |              |      1 |   4171 |    16M| 89666   (2)| 00:17:56 |        |      |     99 |00:00:00.59 |      56 |
|   2 |   TABLE ACCESS FULL| TSMALL_LOCAL |      1 |     99 |   195K|    17   (0)| 00:00:01 |        |      |     99 |00:00:00.01 |      56 |
|   3 |   REMOTE           | TLARGE       |     99 |     42 | 84630 |   900   (1)| 00:00:11 | LOOPB~ | R->S |     99 |00:00:00.59 |       0 |
---------------------------------------------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   2 - SEL$1 / LCL@SEL$1
   3 - SEL$1 / RMT@SEL$1

Remote SQL Information (identified by operation id):
----------------------------------------------------

   3 - SELECT /*+ USE_NL ("RMT") */ "ID","DESCR" FROM "TLARGE" "RMT" WHERE "ID"=:1 (accessing 'LOOPBACK' )
	
	
	-- AS DOKIMASOUME ME MIA DIKH MOY FUNCTION
		
create or replace function myfunc(x_in number) return number
is
begin
	return x_in;
end;
/
  
select /*+ leading(lcl) use_nl(rmt) */ *
from tsmall_local lcl, tlarge@loopback rmt
where
	myfunc(lcl.id) = rmt.id
	
	-- BINGO!! DOULEPSE!
	-- DES TO FILTER OPERATION KAI TO QUERY POU STELNEI STO REMOTE SITE (xvris to predicate me to bind variable anymore)
	
nikos@NIKOSDB> explain plan for
  2  select /*+ leading(lcl) use_nl(rmt) */ *
  3  from tsmall_local lcl, tlarge@loopback rmt
  4  where
  5     myfunc(lcl.id) = rmt.id
  6  /

Explained.

Elapsed: 00:00:00.12
nikos@NIKOSDB> select * from table(dbms_xplan.display)
  2  /

PLAN_TABLE_OUTPUT
----------------------------------------------------------------------------------------------------
Plan hash value: 2213572039

---------------------------------------------------------------------------------------------------
| Id  | Operation          | Name         | Rows  | Bytes | Cost (%CPU)| Time     | Inst   |IN-OUT|
---------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |              |  4171 |    16M| 89666   (2)| 00:17:56 |        |      |
|   1 |  NESTED LOOPS      |              |  4171 |    16M| 89666   (2)| 00:17:56 |        |      |
|   2 |   TABLE ACCESS FULL| TSMALL_LOCAL |    99 |   195K|    17   (0)| 00:00:01 |        |      |
|*  3 |   FILTER           |              |    42 | 84630 |   900   (1)| 00:00:11 |        |      |
|   4 |    REMOTE          | TLARGE       |       |       |            |          | LOOPB~ | R->S |
---------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

   3 - filter("RMT"."ID"="MYFUNC"("LCL"."ID"))

Remote SQL Information (identified by operation id):
----------------------------------------------------

   4 - SELECT /*+ USE_NL ("RMT") */ "ID","DESCR" FROM "TLARGE" "RMT" (accessing 'LOOPBACK'
       )


23 rows selected.

-- @xplan (with statistics_level = ALL)

Plan hash value: 2213572039

---------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name         | Starts | E-Rows |E-Bytes| Cost (%CPU)| E-Time   | Inst   |IN-OUT| A-Rows |   A-Time   | Buffers |
---------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |              |      1 |        |       | 89666 (100)|          |        |      |     99 |00:00:08.53 |      56 |
|   1 |  NESTED LOOPS      |              |      1 |   4171 |    16M| 89666   (2)| 00:17:56 |        |      |     99 |00:00:08.53 |      56 |
|   2 |   TABLE ACCESS FULL| TSMALL_LOCAL |      1 |     99 |   195K|    17   (0)| 00:00:01 |        |      |     99 |00:00:00.01 |      56 |
|*  3 |   FILTER           |              |     99 |     42 | 84630 |   900   (1)| 00:00:11 |        |      |     99 |00:00:08.53 |       0 |
|   4 |    REMOTE          | TLARGE       |     99 |        |       |            |          | LOOPB~ | R->S |    989K|00:00:06.58 |       0 |
---------------------------------------------------------------------------------------------------------------------------------------------

Query Block Name / Object Alias (identified by operation id):
-------------------------------------------------------------

   1 - SEL$1
   2 - SEL$1 / LCL@SEL$1
   3 - SEL$1 / RMT@SEL$1

Predicate Information (identified by operation id):
---------------------------------------------------

   3 - filter("RMT"."ID"="MYFUNC"("LCL"."ID"))

Remote SQL Information (identified by operation id):
----------------------------------------------------

   4 - SELECT /*+ USE_NL ("RMT") */ "ID","DESCR" FROM "TLARGE" "RMT" (accessing 'LOOPBACK' )
	
	
