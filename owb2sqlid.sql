/*************** 
	It returns the mapping between a OWB object (procedure or mapping - essentially a leaf node in a OWB flow) and its corresponding SQL_ID
	It can receive as input the mapping/procedure name and it will return the SQL_ID and vice-versa.
	If none of the two is specified it will return all valid pairs of (mapping/procedure, SQL_ID) found in ASH in the history of interest.
	

	This query selects from DBA_HIST_ACTIVE_SESS_HISTORY in order to return a single row per pair of PLSQL_ENTRY_OBJECT_ID, sql_id
	for the history of interest. A PLSQL_ENTRY_OBJECT_ID is the topmost package in the calling stack of an SQL executing and essentially corresponds to an OWB mapping or procedure.
	Since there might be several sql_ids, related to a PLSQL_ENTRY_OBJECT_ID, the query selects only one of them (the one with the most count(*) of ASH samples - 
	since this is the heaviest from a performance perspective and is most likely the one we are interested in).
	
	NOTE:
		Instead of specifying a specific node, this query can be used on a join on the object_name column, with OWB executions table so as to get 
		the sql_id of specific OWB mappings/procedures. Note that when a procedure e.g. PROC, is called in parallel (e.g., 4 times) by Oracle workflow,
		then OWF changes its name  like this (PROC_01, PROC_02, PROC_03, PROC_04). For example this takes place in the main flow USAGE_EXTRACTION_PF
		for the procedure  ETL_DW.EXTRACTION_CDR.CDR_EXTRACTION:
		
		CDR_EXTRACTION_03
		CDR_EXTRACTION_02
		CDR_EXTRACTION_05
		CDR_EXTRACTION_04
		CDR_EXTRACTION_01
		CDR_EXTRACTION_06
		
		So in owbsys.all_rt_audit_executions the node name appears with the _XX suffix. Please take this into account when joining on the node name.	
	
	Parameters:
		node_name		The name of an OWB mapping/procedure (essentially the name of a leaf node in an OWB flow)
		owner			(optional)	The owner of the OWB mapping/procedure (only works for 'PERIF' or 'ETL_DW')
		sql_id		(optional)	Specify an sql_id to find the corresponding OWB mapping/procedure
		days_back     (optional)  Num of days back from sysdate of required history ASH history (from when the query started execution)
		monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
		Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays
	
***************/

REM @@sqlplus_settings_store

REM set    longchunksize 1000000
REM set    linesize 9999
REM set    long 1000000

col username format a8
col owb_object_type format a20
col sql_opname format a15
col sql_text format a130 trunc
col object_type format a11
col procedure_name format a30
col object_name format a30

undef node_name
undef owner
undef days_back
undef monthly_only
undef mondays_only


--select PLSQL_ENTRY_OBJECT_ID, sql_id, count(*)
--from (
select /*+ parallel(32) */
	decode(USER_ID, 3839, 'ETL_DW', 3980, 'PERIF') username,
	t3.SQL_ID,
	t3.SQL_PLAN_HASH_VALUE LAST_SQL_PLAN_HASH_VALUE,
	--(select object_name from dba_procedures where object_id = t3.PLSQL_ENTRY_OBJECT_ID and subprogram_id = t3.PLSQL_ENTRY_SUBPROGRAM_ID) object_name,
	--(select object_type from dba_procedures where object_id = t3.PLSQL_ENTRY_OBJECT_ID and subprogram_id = t3.PLSQL_ENTRY_SUBPROGRAM_ID) object_type,
	--(select procedure_name from dba_procedures where object_id = t3.PLSQL_ENTRY_OBJECT_ID and subprogram_id = t3.PLSQL_ENTRY_SUBPROGRAM_ID) procedure_name,
	p.object_name,
	case 	when p.object_type = 'PACKAGE' AND p.procedure_name = 'MAIN' THEN 'Mapping'  
			when p.object_type = 'PACKAGE' AND p.procedure_name <> 'MAIN' THEN 'Procedure'
			else p.object_type
	end owb_object_type,
	p.object_type,
	p.procedure_name,
	SQL_OPNAME,
	t3.sql_exec_start last_exec_start,
	t3.sample_time last_sample_time,
	-- convert CLOB to VARCHAR2 because in some cases I got an ORA-22275: invalid LOB locator specified
	case when s.sql_id is not null then dbms_lob.substr( s.sql_text, 130, 1 ) --s.SQL_TEXT 
			else null end sql_text,
	PLSQL_ENTRY_OBJECT_ID,
	PLSQL_ENTRY_SUBPROGRAM_ID
	--t3.*
FROM (
	select -- for each mapping/procedure find the sql_id with the most samples in ASH (or most DB time)
		row_number() over(partition by PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID order by dbtime desc, sample_time desc) r,
		t2.*
	from (	
		select 
			sum(10) over(partition by T.PLSQL_ENTRY_OBJECT_ID, SQL_ID) dbtime,
			t.*
		from dba_hist_active_sess_history t
		where 
			T.PLSQL_ENTRY_OBJECT_ID = (select distinct object_id from dba_procedures where (object_name =  upper(trim('&&node_name')) OR procedure_name =  upper(trim('&&node_name'))) and owner = nvl(upper('&&owner'),owner)) 
			AND T.PLSQL_ENTRY_SUBPROGRAM_ID in (select subprogram_id from dba_procedures where (object_name =  upper(trim('&&node_name')) OR procedure_name =  upper(trim('&&node_name'))) and owner = nvl(upper('&&owner'),owner))
			AND T.SQL_ID = 	nvl('&sql_id', T.SQL_ID)
			-- basic filters
			AND user_id in (3839, 3980) -- ETL_DW, PERIF
			and T.IN_SQL_EXECUTION = 'Y'
			and T.IN_PLSQL_EXECUTION = 'N'
			and T.IS_SQLID_CURRENT = 'Y'
			and T.PLSQL_ENTRY_OBJECT_ID is not null
			and T.PLSQL_ENTRY_SUBPROGRAM_ID is not null
			and SQL_ID is not null
			--and SQL_OPCODE in (2, 7, 6, 189, 1) -- in ('INSERT', 'DELETE', 'UPDATE', 'UPSERT', 'CREATE TABLE')
			and t.sql_exec_start > sysdate - &days_back
			AND to_char(t.sql_exec_start, 'DD') = case when nvl('&monthly_only',0) = 1 then '01' else to_char(t.sql_exec_start, 'DD') end    
			AND trim(to_char(t.sql_exec_start, 'DAY')) = case when nvl('&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(t.sql_exec_start, 'DAY')) end
	)t2        
)t3, dba_procedures p, DBA_HIST_SQLTEXT s
where
	r=1
	AND p.object_id = t3.PLSQL_ENTRY_OBJECT_ID and p.subprogram_id = t3.PLSQL_ENTRY_SUBPROGRAM_ID
	and t3.sql_id = s.sql_id (+)
order by 1,3
--)
--group by PLSQL_ENTRY_OBJECT_ID, SQL_ID
--having count(*) > 1
/

REM @@sqlplus_get_settings

/*
For testing:

EXTRACT_DAILY_ORDERS procedure

REFRESH_KPIDW_MVFAULT_PROC  procedure  

REFRESH_CRITEVENT_MV1_PROC  procedure

WFM_ONLINE_PRERUNCHECK  procedure

KPIMR_DAILYDET_LOAD_DLY_SNP mapping

SNCH_OPEN_STRATEGY_LD_PR  mapping

POPULATE_IPTV_ORDERS_MON_SNP  procedure

UPD_WFM_ONLINE_IFCNTL_TBL  procedure

SNCH_WORK_ITEMS_FCT_UPD  mapping

SNCH_CUSTOMER_NU_EXT  mapping
*/