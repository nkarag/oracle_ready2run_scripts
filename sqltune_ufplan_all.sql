------------------------------------------------------------------------------------------------
-- sqltune_ufplan_all.sql
--
-- Description
--	Unforce all ACCEPTED plans  for a specific sql_id from the SQL Plan Baseline.
--
-- Prerequisites
--	The sql text must be loaded either in memory (v$sqlarea), or in AWR (DBA_HIST_SQLTEXT) so as to compute the singature.
--
-- Parameters
--	1.	sql_id	The input sql id.
--
-- (C) 2014 Nikos Karagiannidis - http://oradwstories.blogspot.com  
------------------------------------------------------------------------------------------------

PRO Parameter 1:
PRO TARGET_SQL_ID (required)
PRO
DEF target_sql_id = '&1';

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
declare
	no_plans PLS_INTEGER := 0;
begin
	:phv := 0;
	-- loop through (accepted only) plans in the sql plan baseline of this signature and get the sql_handle and plan_name so as to drop the correspondng plan
	for r in (
		select SQL_HANDLE,PLAN_NAME
		from dba_sql_plan_baselines
		where
			SIGNATURE = :signature
		AND	ACCEPTED = 'YES'
	) LOOP
		:sql_handle := r.sql_handle;
		:plan_name := r.plan_name;
		-- drop plan
		no_plans := DBMS_SPM.DROP_SQL_PLAN_BASELINE (sql_handle => :sql_handle, plan_name => :plan_name);
	
		if (no_plans <> 1) then
			RAISE_APPLICATION_ERROR(-20102, '***Error***: '||no_plans||' number of plans have been dropped for sql_handle = '||:sql_handle||' and plan_name = '||:plan_name);	
		end if;	
	END LOOP;
end;
/

undef target_sql_id
undef plan_hash_value