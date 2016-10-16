----------------------------------------------------------------------------------------
-- fsqlprofile.sql
--
-- Description
--	Find the SQL Profile for a specific sql_id.
--
-- Note
--	the script finds the sql_text of this sql_id either in memory (gv$sqlarea) or in AWR and from
--	this it computes the corresponding signature (with AND without force matching). With these two singatures it searches dba_sql_profiles
--	to retrieve the available profile.
--
-- Prerequisites
--	The sql must be loaded either in memory (gv$sqlarea), or in AWR (DBA_HIST_SQLTEXT)
--
-- Parameters
--	1.	sql_id	The input sql id.
--
--	Author
--		Nikos Karagiannidis (C) 2014 - http://oradwstories.blogspot.gr/
----------------------------------------------------------------------------------------

undef target_sql_id

var sql_text clob;
var signature number;
var signature_fm number;

-- get sql_text from memory
BEGIN
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
	
	-- get the signature for this sql_id (with force matching)
	select DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE (sql_text  =>:sql_text, force_match => 1) into :signature_fm from dual;
	
END;
/


col DESCRIPTION format a150
col sql_text format a130 trunc


-- return the sql_profile
select	
	'&&target_sql_id.' sql_id,
	SIGNATURE,
	NAME PROFILE_NAME,
	CATEGORY,
	TYPE,
	STATUS,
	FORCE_MATCHING,
	DESCRIPTION,
	to_char(CREATED, 'dd/mm/yyyy hh24:mi:ss') CREATED,
	to_char(LAST_MODIFIED, 'dd/mm/yyyy hh24:mi:ss') LAST_MODIFIED,
	TASK_ID,
	TASK_EXEC_NAME,
	TASK_OBJ_ID,
	TASK_FND_ID,
	TASK_REC_ID,
	SQL_TEXT
from dba_sql_profiles
where
	SIGNATURE IN (:signature, :signature_fm)
order by CREATED DESC
/	
	
