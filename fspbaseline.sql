----------------------------------------------------------------------------------------
-- fspbaseline.sql
--
-- Description
--	Find the SQL Plan Baseline  plans (all plans ACCEPTED or not) for a specific sql_id.
--
-- Note
--	the script finds the sql_text of this sql_id either in memory (gv$sqlarea) or in AWR and from
--	this it computes the corresponding signature (with no force matching). With the singature it searches
--	the corresponding sql plan baseline to retrieve all available plans.
--
-- Prerequisites
--	The sql must be loaded either in memory (gv$sqlarea), or in AWR (DBA_HIST_SQLTEXT)
--
-- Parameters
--	1.	sql_id	The input sql id.
--
--	Nikos Karagiannidis (C) 2014 - http://oradwstories.blogspot.gr/
----------------------------------------------------------------------------------------

PRO	Parameter 1:
PRO SQL_ID (required)
DEF target_sql_id = '&1';

var sql_text clob;
var signature number;

-- get sql_text
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

	-- get the signature for this sql_id (with no force matching)
	select DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE (sql_text  =>:sql_text, force_match => 0) into :signature from dual;
END;
/

col ENABLED format a10
col ACCEPTED format a10
col FIXED format a10
col REPRODUCED format a10
col DESCRIPTION format a100
col sql_text format a130 trunc
col CREATOR format a20
col ORIGIN format a20
col PARSING_SCHEMA_NAME format a30

-- return the baseline
select	
	'&&target_sql_id.' sql_id,
	SIGNATURE,
	SQL_HANDLE,
	PLAN_NAME,
	ENABLED,
	ACCEPTED,
	FIXED,
	--REPRODUCED, -- check https://blogs.oracle.com/optimizer/entry/how_does_sql_plan_management
	DESCRIPTION,
	CREATOR,
	ORIGIN,
	PARSING_SCHEMA_NAME,
	SQL_TEXT	
from dba_sql_plan_baselines
where
	SIGNATURE = :signature
order by FIXED DESC, ACCEPTED DESC	
/	

undef target_sql_id
	
