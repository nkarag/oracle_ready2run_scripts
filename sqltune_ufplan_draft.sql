------------------------------------------------------------------------------------------------
-- sqltune_ufplan.sql
--
-- Description
--	"Unfix" the plan of a specific sql_id by dropping the specific (accepted) plan from the SQL Plan Baseline.
--
-- Prerequisites
--	The sql text must be loaded either in memory (v$sqlarea), or in AWR (DBA_HIST_SQLTEXT) so as to compute the singature.
--
-- Parameters
--	1.	sql_id	The input sql id.
--  2.	hash plan value of the plan to be dropped
--
--	Author
--		Nikos Karagiannidis (C) 2014 - http://oradwstories.blogspot.gr/
------------------------------------------------------------------------------------------------

PRO Parameter 1:
PRO TARGET_SQL_ID (required)
PRO
DEF target_sql_id = '&1';
PRO Parameter 2:
PRO PLAN_HASH_VALUE (required)
PRO
DEF plan_hash_value = '&2';

var sql_text clob;
var signature number;
var phv	number;
var	sql_handle	VARCHAR2(30);
var plan_name	VARCHAR2(30);

-- 1. find sql text either in memory (gv$sqlarea) or in AWR (dba_hist_sqltext)
BEGIN
	-- get sql_text from memory
	BEGIN
	  SELECT REPLACE(sql_fulltext, CHR(00), ' ')
		INTO :sql_text
		FROM gv$sqlarea
	   WHERE sql_id = TRIM('&&target_sql_id.')
		 AND ROWNUM = 1;
	EXCEPTION
	  WHEN OTHERS THEN
		DBMS_OUTPUT.PUT_LINE('getting target sql_text from memory: '||SQLERRM);
		:sql_text := NULL;
	END;	
	 
	-- get sql_text from awr
	BEGIN
	  IF :sql_text IS NULL OR NVL(DBMS_LOB.GETLENGTH(:sql_text), 0) = 0 THEN
		SELECT REPLACE(sql_text, CHR(00), ' ')
		  INTO :sql_text
		  FROM dba_hist_sqltext
		 WHERE sql_id = TRIM('&&target_sql_id.')
		   AND sql_text IS NOT NULL
		   AND ROWNUM = 1;
	  END IF;
	EXCEPTION
	  WHEN OTHERS THEN
		DBMS_OUTPUT.PUT_LINE('getting target sql_text from awr: '||SQLERRM);
		:sql_text := NULL;
	END;
	 
	-- sql_text as found
	--	SELECT :sql_text FROM DUAL;
	 
	-- check is sql_text for target sql is available
	--	SET TERM ON;
	BEGIN
	  IF :sql_text IS NULL THEN
		RAISE_APPLICATION_ERROR(-20100, 'SQL_TEXT for target SQL_ID &&target_sql_id. was not found in memory (gv$sqlarea) or AWR (dba_hist_sqltext).');
	  END IF;
	END;
END;
/	
-- 2. Calculate from sql text the signature (no force matching)
exec	select DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE (sql_text  =>:sql_text, force_match => 0) into :signature from dual

-- 3. For the specific signature get all available accepted plans in dba_sql_plan_baseline 
--    and find the one corresponding to the input plan hash value.
begin
	:phv := 0;
	-- loop through (accepted only) plans in the sql plan baseline of this signature and get the sql_handle and plan_name
	for r in (
		select SQL_HANDLE,PLAN_NAME,
		from dba_sql_plan_baselines
		where
			SIGNATURE = :signature
		AND	ACCEPTED = 'YES'
	) LOOP
		-- get the plan hash value
			select regexp_replace(plan_table_output, '^Plan hash value:\s+','') plan_hash_value, plan_table_output 
				into :phv
			from table( dbms_xplan.DISPLAY_SQL_PLAN_BASELINE(sql_handle =>r.SQL_HANDLE, plan_name => r.PLAN_NAME,  format=>'BASIC'))
			where 
				plan_table_output like 'Plan hash value%';
			
			if(:phv = '&&PLAN_HASH_VALUE.') then
				:sql_handle := r.sql_handle;
				:plan_name := r.plan_name;
				exit;					
			end if;
	END LOOP;
	
	if (:phv <> '&&PLAN_HASH_VALUE.') then
		RAISE_APPLICATION_ERROR(-20101, 'The requested plan &&PLAN_HASH_VALUE. was not found dba_sql_plan_baselines');
	end if;
end;
/

-- 4. then drop this plan
declare
	no_plans PLS_INTEGER;
begin
	no_plans := DBMS_SPM.DROP_SQL_PLAN_BASELINE (sql_handle => :sql_handle, plan_name => :plan_name);
	
	if (no_plans <> 1) then
		RAISE_APPLICATION_ERROR(-20102, 'Error: '||no_plans||' number of plans have been dropped.');	
	end if;
end;
/	

undef target_sql_id



---****************************** DRAFT

-- how to get the hash plan value out of the sql plan baseline plan_name and sql_handle
select regexp_replace(plan_table_output, '^Plan hash value:\s+','') plan_hash_value, plan_table_output 
from table( dbms_xplan.DISPLAY_SQL_PLAN_BASELINE(sql_handle =>'&sql_handle', plan_name => '&plan_name',  format=>'BASIC'))
where 
    plan_table_output like 'Plan hash value%';

nikos@NIKOSDB> select *
  2  FROM dba_sql_plan_baselines
  3  /

 SIGNATURE SQL_HANDLE                     SQL_TEXT                                                                 PLAN_NAME                      CREATOR                        ORIGIN         PARSING_SCHEMA_NAME            DESCRIPTION                                                                                                                                                                                                                                                                                                                                                                                                                                                                          VERSION                                                  CREATED                                                             LAST_MODIFIED                                                               LAST_EXECUTED                                                               LAST_VERIFIED                                                       ENA ACC FIX AUT OPTIMIZER_COST MODULE                                   ACTION                           EXECUTIONS ELAPSED_TIME   CPU_TIME BUFFER_GETS DISK_READS DIRECT_WRITES ROWS_PROCESSED    FETCHES END_OF_FETCH_COUNT
---------- ------------------------------ -------------------------------------------------------------------------------- ------------------------------ ------------------------------ -------------- ------------------------------ -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ---------------------------------------------------------------- --------------------------------------------------------------------------- --------------------------------------------------------------------------- --------------------------------------------------------------------------- --------------------------------------------------------------------------- --- --- --- --- -------------- ------------------------------------------------ -------------------------------- ---------- ------------ ---------- ----------- ---------- ------------- -------------- ---------- ------------------
5.0715E+18 SYS_SQL_46619055d11c086e       select OWNER, OBJECT_NAME, CREATED                                       SQL_PLAN_4cschar8js23f7589196e NIKOS                          MANUAL-LOAD    NIKOS                  ORIGINAL:DGZ8CS2P1RBNK MODIFIED:2JZGRDAUGY6TK PHV:3779072106 CREATED BY COE_LOAD_SQL_BASELINE.SQL                                                                                                                                                                                                                                                                                                                                                                                            11.2.0.1.0                                                       28-AUG-14 07.15.48.000000 PM                                        28-AUG-14 07.15.48.000000 PM                                                28-AUG-14 07.16.54.000000 PM                                                                                                                    YES YES NO  YES            321 SQL*Plus                                                                           1        19730      31200        1226          0             0              9          2                  1
5.0715E+18 SYS_SQL_46619055d11c086e       select OWNER, OBJECT_NAME, CREATED                                       SQL_PLAN_4cschar8js23f2a4165ac NIKOS                          AUTO-CAPTURE   NIKOS                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       11.2.0.1.0                                               28-AUG-14 07.16.54.000000 PM                                        28-AUG-14 07.16.54.000000 PM                                                                                                                                                                                        YES NO  NO  YES              2 SQL*Plus                                                                           0            0          0           0          0             0       0         0                  0
1.1470E+19 SYS_SQL_9f2bdd863f2ab67f         SELECT ch.channel_class, c.cust_city, t.calendar_quarter_desc,                 SQL_PLAN_9yayxhszkpdmz144094c5 SYS                            MANUAL-LOAD    SH                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  11.2.0.1.0                                                       27-JUL-14 04.42.43.000000 PM                                        27-JUL-14 04.42.43.000000 PM                                             27-JUL-14 04.44.07.000000 PM                                                                                                               YES YES NO  YES              6 SQL*Plus                                                                           0            0          0           0          0             0              0          0                  0
1.1470E+19 SYS_SQL_9f2bdd863f2ab67f         SELECT ch.channel_class, c.cust_city, t.calendar_quarter_desc,                 SQL_PLAN_9yayxhszkpdmzde7d8b66 SYS                            AUTO-CAPTURE   SH                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  11.2.0.1.0                                                       27-JUL-14 04.44.07.000000 PM                                        27-JUL-14 04.44.07.000000 PM                                                                                                                                                                                        YES NO  NO  YES              5 sqlplus.exe                                                                        0            0          0           0          0             0              0          0                  0

Elapsed: 00:00:00.20

SQL_PLAN_4cschar8js23f7589196e

var stext clob;

exec :stext := 'select OWNER, OBJECT_NAME, CREATED from test_mplan where object_type = ''WINDOW''';

nikos@NIKOSDB> select DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE (sql_text  =>:stext, force_match => 1) from dual
  2  /

DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE(SQL_TEXT=>:STEXT,FORCE_MATCH=>1)
------------------------------------------------------------------
                                                        1.4163E+19

Elapsed: 00:00:00.02
nikos@NIKOSDB> select DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE (sql_text  =>:stext, force_match => 0) from dual
  2  /

DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE(SQL_TEXT=>:STEXT,FORCE_MATCH=>0)
------------------------------------------------------------------
                                                        5.0715E+18
                                                                               *

select DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE (
  sql_text  => ( SELECT REPLACE(sql_text, CHR(00), ' ')      
      FROM dba_hist_sqltext
     WHERE sql_id = TRIM('&&target_sql_id.')
       AND sql_text IS NOT NULL
       AND ROWNUM = 1)
  ,  force_match => FALSE)
from dual;  


SELECT signature, sql_handle, plan_name 
FROM dba_sql_plan_baselines WHERE rownum <= 3;
--****************************************************************************


-- GET the sql text from the sql_id
VAR sql_text CLOB;
VAR plan_name VARCHAR2(30);
EXEC :sql_text := NULL;
EXEC :plan_name := NULL;
 
-- get sql_text from memory
BEGIN
  SELECT REPLACE(sql_fulltext, CHR(00), ' ')
    INTO :sql_text
    FROM gv$sqlarea
   WHERE sql_id = TRIM('&&target_sql_id.')
     AND ROWNUM = 1;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('getting target sql_text from memory: '||SQLERRM);
    :sql_text := NULL;
END;
/
 
-- get sql_text from awr
BEGIN
  IF :sql_text IS NULL OR NVL(DBMS_LOB.GETLENGTH(:sql_text), 0) = 0 THEN
    SELECT REPLACE(sql_text, CHR(00), ' ')
      INTO :sql_text
      FROM dba_hist_sqltext
     WHERE sql_id = TRIM('&&target_sql_id.')
       AND sql_text IS NOT NULL
       AND ROWNUM = 1;
  END IF;
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('getting target sql_text from awr: '||SQLERRM);
    :sql_text := NULL;
END;
/
 
-- sql_text as found
SELECT :sql_text FROM DUAL;
 
-- check is sql_text for target sql is available
SET TERM ON;
BEGIN
  IF :sql_text IS NULL THEN
    RAISE_APPLICATION_ERROR(-20100, 'SQL_TEXT for target SQL_ID &&target_sql_id. was not found in memory (gv$sqlarea) or AWR (dba_hist_sqltext).');
  END IF;
END;
/

-- get the signature from the sql_text

-- get the sql_handle and plan name from dba_sql_plan_baselines

/* declare
	sqlhandle	VARCHAR2(30);
	planname	VARCHAR2(30);
	signature	NUMBER;
	sqltext		CLOB;
begin	
	-- get the sqltext
	select
	
	
	-- find the sql_handle and the plan_name
	select into sqlhandle, planname
	from dba_sql_plan_baselines
	where
		

end; */


undef target_sql_id
undef plan_hash_value