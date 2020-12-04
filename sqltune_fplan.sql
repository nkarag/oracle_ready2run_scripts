-- ----------------------------------------------------------------------------------------------
-- Fix the plan for a specific sql_id. Loads the plan into an SQL Plan Baseline (thus works for 11g and above only)
-- Input the sql_id and the requested plan, as well as the number of days back to search AWR for this plan (if it is
-- not loaded in the cursor cache). 
--
-- DESCRIPTION
--	This script uses the concept of the SQL plan baseline in order to fix the plan of a specific sql id.
--	"Fixing" here is not with the strict meaning of SQL plan baselines "fixed plans". The script loads
--	the input plan into an SQL Plan Baseline (if one does not exist then Oracle will create one) as an
--	"enabled" and "accepted" plan. The plan (by default) will not be "fixed", which means that we will allow to the
--	SQL Plan Baseline to evolve (see http://oradwstories.blogspot.gr/2014/07/sql-plan-management-sql-plan-baselines.html
--	for more info), unless you provide "Y" in the FIXED input parameter. The plan must be loaded either in the cursor cache, or in AWR for the script to work.
--
-- PRE-REQUISITES
--   1. Have in cache or AWR the input plan.
--   2. Have in cache the input plan
--
-- PARAMETERS
--   1. SQL_ID (required)
--   2. PLAN_HASH_VALUE (required)
--	 3.	DAYS_BACK (optional, default 10)
--	 4. FIXED (optional, default N)
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

@sqlplus_settings_store

set serveroutput on
set timing off
set verify off
set feedback off

-- get the input
PRO
PRO Parameter 1:
PRO SQL_ID (required)
DEF sql_id = '&1';
PRO
PRO Parameter 2:
PRO PLAN_HASH_VALUE (required)
DEF plan_hash_value = '&2';
PRO
PRO Parameter 3:
ACCEPT	days_back NUMBER DEFAULT 10 PROMPT 'Specify the number of days back from SYSDATE for which you want to search the AWR (default 10):'
PRO
PRO Parameter 4:
ACCEPT	fixed CHAR  PROMPT 'Loaded plan is used as a fixed plan Y/N (default N):'

VARIABLE	g_app_error_flag NUMBER
EXEC	:g_app_error_flag := 0;

-- Check if the plan is in the CURSOR CACHE
VARIABLE g_plan_found_in_cache NUMBER
EXEC :g_plan_found_in_cache := 0

BEGIN
	select count(*) into :g_plan_found_in_cache
	from gv$sql
	where 1=1
		and	sql_id = trim('&&sql_id')
		and	plan_hash_value = &&plan_hash_value;				

	if(:g_plan_found_in_cache > 0) then
		DBMS_OUTPUT.PUT_LINE(chr(10)||'***INFO***:	The requested plan was found in cursor cache (GV$SQL).');
	else
		DBMS_OUTPUT.PUT_LINE(chr(10)||'***INFO***:	The requested plan was NOT found in cursor cache (GV$SQL).');
	end if;
END;
/

var g_sqlid VARCHAR2(30)
exec :g_sqlid := trim('&&sql_id')

var g_phv NUMBER
exec :g_phv := &&plan_hash_value

var g_fixed	VARCHAR2(15)
exec select decode(upper(trim(nvl('&&fixed', 'N'))), 'Y', 'YES', 'N', 'NO', 'invalid value') into :g_fixed from dual

-- if the plan is in the cursor cache, then load it directly in a SQL plan baseline
declare
	l_i	pls_integer;
	--g_sqlid varchar2(30) := trim('&&sql_id');
	--g_phv	number := &&plan_hash_value;
begin
-- DEBUG -------------------------------------
--DBMS_OUTPUT.PUT_LINE('***DEBUG***:	fixed = '||:g_fixed);
----------------------------------------------
	if(:g_plan_found_in_cache > 0) then
		-- load plan from cursor cache
		l_i:= DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE (
			sql_id  => :g_sqlid,
			plan_hash_value   => :g_phv,
			fixed => :g_fixed);
			
		if (l_i <> 1) then
			--:g_app_error_flag := 1;
			--RAISE_APPLICATION_ERROR(-20100, '***ERROR***: Problem in loading plan from cursor cache into SQL plan baseline.'||l_i||' number of plans have been loaded.');			
			DBMS_OUTPUT.PUT_LINE(chr(10)||'***ERROR***: Problem in loading plan from cursor cache into SQL plan baseline.'||l_i||' number of plans have been loaded.');			
			DBMS_OUTPUT.PUT_LINE(chr(10)||'***INFO***: We will continue search in AWR ...');
			-- set flags to alllow search in AWR
			:g_plan_found_in_cache := 0;
			:g_app_error_flag := 0;
		end if;
	end if;
end;
/

-- if the plan is not in the cursor cache, then we need to find it in AWR
begin
	if(:g_plan_found_in_cache = 0 AND :g_app_error_flag = 0) then
		DBMS_OUTPUT.PUT_LINE(chr(10)||'***INFO***:	The requested plan must be searched in AWR (DBA_HIST_SQLSTAT)');
	end if;
end;
/

var g_plan_found_in_awr number
exec :g_plan_found_in_awr := 0
var g_ps_name VARCHAR2(60)
var	g_bsnap	number
var	g_esnap	number	
begin
	if(:g_plan_found_in_cache = 0 AND :g_app_error_flag = 0) then
		-- need to get a Begin and an End snapshot id, need to get the parsing schema name also to be used as the STS owner
		select min(snap_id) begin_snap, max(snap_id) end_snap, min(PARSING_SCHEMA_NAME) ps_name into :g_bsnap, :g_esnap, :g_ps_name
		from	DBA_HIST_SQLSTAT a left outer join
					DBA_HIST_SNAPSHOT b using (SNAP_ID)  
		where 1=1
			and sql_id = :g_sqlid
			and plan_hash_value = :g_phv
			and b.begin_interval_time > sysdate - &&days_back;		
			
		 if(:g_bsnap is null or :g_esnap is null) then 
			:g_app_error_flag := 1;
			RAISE_APPLICATION_ERROR(-20101, '***ERROR***: Problem in finding plan in DBA_HIST_SQLSTATS');
		 end if;

		 -- in order to avoid an: ORA-13767: End snapshot ID must be greater than begin snapshot ID.
		 -- if the snap_ids are the same, then lower the begin snap id by one
		if(:g_bsnap = :g_esnap) then
			:g_bsnap := :g_bsnap - 1;
		end if;
		
		SELECT count(*) into :g_plan_found_in_awr
		FROM TABLE (DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY (
					  begin_snap      => :g_bsnap,
					  end_snap        => :g_esnap,
					  basic_filter => 'sql_id = '''||:g_sqlid||''' AND plan_hash_value = '||:g_phv)) p;
					  
		if (:g_plan_found_in_awr = 0) then
			:g_app_error_flag := 1;
			RAISE_APPLICATION_ERROR(-20102, '***ERROR***: Problem in finding plan in AWR (DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY)');
		else
			DBMS_OUTPUT.PUT_LINE(chr(10)||'***INFO***:	The requested plan was found in AWR.');
		end if;
	end if;
end;
/

-- if plan is found in AWR, then load it via an sql tuning set (STS)
declare
	l_mycursor DBMS_SQLTUNE.SQLSET_CURSOR;
	l_stsname	VARCHAR2(100) := 'sts_for_sqlid_'||:g_sqlid;
	l_i	pls_integer;
begin
	if (:g_plan_found_in_awr > 0 AND :g_app_error_flag = 0) then
	
		-- create an STS (SQL Tuning Set) for this 
		DBMS_SQLTUNE.CREATE_SQLSET(
			sqlset_name => l_stsname, 
			description  => 'STS for loading a plan into a SQL plan baseline from AWR',
			sqlset_owner => :g_ps_name);	
		
		-- load the STS from AWR (you need to specify the begin and end snaphsots)
		 OPEN l_mycursor FOR
			SELECT VALUE(p)
			FROM TABLE (DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY (
					  begin_snap      => :g_bsnap,
					  end_snap        => :g_esnap,
					  basic_filter => 'sql_id = '''||:g_sqlid||''' AND plan_hash_value = '||:g_phv)) p;

		DBMS_SQLTUNE.LOAD_SQLSET(
					 sqlset_name     => l_stsname,
					 populate_cursor => l_mycursor,
					 sqlset_owner => :g_ps_name);									 

		-- Create the Baseline from the STS					 
		l_i := DBMS_SPM.LOAD_PLANS_FROM_SQLSET (
				sqlset_name    => l_stsname,
				sqlset_owner   => :g_ps_name,
				fixed => :g_fixed);		
				
		if (l_i <> 1) then
			:g_app_error_flag := 1;
			RAISE_APPLICATION_ERROR(-20103, '***ERROR***: Problem in loading plan from sql tuning set (STS) into SQL plan baseline.'||l_i||' number of plans have been loaded.');
		end if;		
	end if;
end;
/	

begin
	if(:g_app_error_flag = 0) then
		DBMS_OUTPUT.PUT_LINE(chr(10)||'***INFO***:	The requested plan has been loaded into the SQL Plan Baseline. Lets check DBA_SQL_PLAN_BASELINES ...');
	end if;
end;
/	

-- Check creation of the SQL Plan baseline. 
-- 	I need the sql signature in order to query dba_sql_plan_baseline. I will also
-- 	get the sql handle and plan name, in order to be able to retrieve the execution plan from dbms_xplan.DISPLAY_SQL_PLAN_BASELINE
-- 	Alternatively call: @@fspbaseline '&&sql_id'
var g_signature NUMBER
declare
	------------------------------------------------------------------
	-- get the signature for input sql_id (with no force matching)
	------------------------------------------------------------------
	function sqlid_2_signature(sql_id_in in varchar2) return number
	is
		l_sql_text	clob;
		l_sign	number;
	begin
		-- get sql_text from memory
		if(:g_plan_found_in_cache > 0) then
			BEGIN
				SELECT REPLACE(sql_fulltext, CHR(00), ' ')	INTO l_sql_text
				FROM gv$sqlarea
				WHERE sql_id = sql_id_in
					AND ROWNUM = 1;
			EXCEPTION
				WHEN OTHERS THEN
					:g_app_error_flag := 1;
					l_sql_text := NULL;
					RAISE_APPLICATION_ERROR(-20104,'***ERROR***: Getting target sql_text from memory: '||SQLERRM);					
			END;				
		-- get sql_text from awr
		elsif (:g_plan_found_in_awr > 0) then
			BEGIN
				SELECT REPLACE(sql_text, CHR(00), ' ') INTO l_sql_text
				FROM dba_hist_sqltext
				WHERE sql_id = sql_id_in
					AND sql_text IS NOT NULL
					AND ROWNUM = 1;
			EXCEPTION
				WHEN OTHERS THEN
					:g_app_error_flag := 1;
					l_sql_text := NULL;
					RAISE_APPLICATION_ERROR(-20105,'***ERROR***: Getting target sql_text from awr: '||SQLERRM);					
			END;		
		-- something has gone wrong!
		else
			:g_app_error_flag := 1;
			l_sql_text := NULL;
			RAISE_APPLICATION_ERROR(-20106, '***ERROR***: Error while trying to get the sql text, Cannot find sqltext neither in memory nor in AWR!');		
		end if;
		
		if(l_sql_text IS NULL) then
			:g_app_error_flag := 1;
			RAISE_APPLICATION_ERROR(-20107, '***ERROR***: Error while trying to get the sql text, Cannot find sqltext neither in memory nor in AWR!');		
		end if;

		-- get the signature for this sql_id (with no force matching)
		l_sign := DBMS_SQLTUNE.SQLTEXT_TO_SIGNATURE (sql_text  =>l_sql_text, force_match => 0);				
		
		return l_sign;
	end;
begin
	if(:g_app_error_flag = 0) then
		:g_signature := sqlid_2_signature(:g_sqlid);
	end if;
end; 
/

-- return the baseline
col ENABLED format a10
col ACCEPTED format a10
col FIXED format a10
col REPRODUCED format a10
col DESCRIPTION format a100
col sql_text format a130 trunc
col CREATOR format a20
col ORIGIN format a20
col PARSING_SCHEMA_NAME format a30

column sqlh new_value sql_handle
column pn new_value plan_name
select *
from (
	select	
		:g_sqlid sql_id,
		SIGNATURE,
		SQL_HANDLE sqlh,
		PLAN_NAME pn,
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
		SIGNATURE = :g_signature
		AND	creator = USER
		AND origin = 'MANUAL-LOAD'
	order by created desc, FIXED DESC, ACCEPTED DESC	
) t
where
	rownum = 1
/	
	
PROMPT
PROMPT *** And the plan is the following ...
PROMPT (note that the plan hash value might be different)
PROMPT

-- Check plan in the SQL Plan baseline
set linesize 9999
set pagesize 999

select * from table( dbms_xplan.DISPLAY_SQL_PLAN_BASELINE(sql_handle =>'&&sql_handle', plan_name => '&&plan_name', format=>'TYPICAL'))
/

UNDEF	sql_id
UNDEF	plan_hash_value
UNDEF	ref_date
UNDEF	days_back
UNDEF	sql_handle
UNDEF	plan_name
UNDEF	fixed
@sqlplus_get_settings 


