col username format a8
col sql_opname format a15
col sql_text format a130 trunc
col object_type format a11
col OBJECT_OWNER format a11
col procedure_name format a30
col object_name format a30
col owb_node_name format a30
col OWB_OBJECT_TYPE format a20
col main_flow format a15
col PROCEDURE_NAME format a30
col DBtime_secs format 999G999G999D99
col avg_dbtime_per_exec_sec format 999G999G999G999
col dbtime_std_dev format 999G999G999G999
col min_dbtime_per_exec_sec format 999G999G999G999
col max_dbtime_per_exec_sec format 999G999G999G999
col NUMOF_EXECUTIONS format 999G999
col avg_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col std_dev_duration format 999G999G999D99 justify right null '<NULL>' 
col max_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col min_duration_mins format 999G999G999D99 justify right null '<NULL>'
col SQL_ID format a16 justify center
col r format 999

undef days_back
undef monthly_only
undef mondays_only

/********************************************
This query returns the topN sql_ids (corresponding to OWB mappings/procedures - leaf nodes in general) 
in the history of interest (&days_back from sysdate), based on ***average DB time*** as this is recorded in DBA_HIST_ACTIVE_SESS_HISTORY.
DB time is measured (sum(10) on ash samples) on a "per execution" basis (i.e., grouped by ash.sql_exec_start) and then averaged for the whole period. 
This way we can more easily identify "heavy" sql_ids and get the (average) elapsed DB time ***per execution***. Also we can measure
the number of executions and thus identify daily/weekly/monthly mappings. Note that ALL px slaves have the same value in sql_exec_start
and thus by aggregating on (sql_id, sql_exec_start) we measure the dbtime for all parallel slaves.

Apart from the average/min/max DB time per execution, the query returns the average/min/max wall-clock time per execution of this node
for the same period of history (from owbsys.all_rt_audit_executions). 

There is also an optional input parameter (&sql_id) that if entered, the query returns the above metrics for a specific sql_id.

	Parameters:
		days_back   (optional)  Num of days back from sysdate of required history ASH history (from when ash sampling started)	
		sql_id		(optional)	Specify an sql_id to find the performance metrics for a specific sql id		
		monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
		Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays
		n			Specify "N" from top N	
*********************************************/

select
		row_number() over(order by avg_dbtime_per_exec_sec desc ) r,
		dbt.avg_dbtime_per_exec_sec, dbt.dbtime_std_dev, dbt.min_dbtime_per_exec_sec, dbt.max_dbtime_per_exec_sec, dbt.numof_executions,
		avg_duration_mins,
		std_dev_duration,
		min_duration_mins,
		max_duration_mins,
		dbt.sql_id,
        dbt.SQL_OPNAME, 
		dbt.username,
		main_flow,            
        dbt.owb_node_name,
        dbt.owb_object_type,
        dbt.object_owner,        
        dbt.object_name,
        dbt.object_type,
        dbt.procedure_name,       
        dbt.sql_text
from (  
	select /*+ parallel(16) */  
			fin.avg_dbtime_per_exec_sec, fin.dbtime_std_dev, fin.min_dbtime_per_exec_sec, fin.max_dbtime_per_exec_sec, fin.numof_executions,
			fin.sql_id,
			fin.SQL_OPNAME, 
			fin.username,
			(   select execution_name 
				from owbsys.all_rt_audit_executions 
				where execution_audit_id =  (
												select max(TOP_LEVEL_EXECUTION_AUDIT_ID) 
												from owbsys.all_rt_audit_executions  
												where -- for the case where OWF appends a _XX suffix to OWB node names
													REGEXP_REPLACE(execution_name, '_\d\d$', '') like '%:'|| fin.owb_node_name 
													--execution_name like '%:'|| fin.owb_node_name
													--SUBSTR (execution_name, INSTR (execution_name, ':') + 1) =  fin.owb_node_name                                                
													AND created_on > sysdate - &&days_back
											)
			) main_flow,            
			fin.owb_node_name,
			fin.owb_object_type,
			fin.object_owner,        
			fin.object_name,
			fin.object_type,
			fin.procedure_name,       
			fin.sql_text
	from (
		select 
			decode(topn.USER_ID, 3839, 'ETL_DW', 3980, 'PERIF', (select username from dba_users where user_id = topn.user_id)) username,
			case    when p.object_type = 'PACKAGE' AND p.procedure_name = 'MAIN' THEN p.object_name  
					when p.object_type = 'PACKAGE' AND p.procedure_name <> 'MAIN' THEN p.procedure_name
					else 'Unknown'
			end owb_node_name,        
			case    when p.object_type = 'PACKAGE' AND p.procedure_name = 'MAIN' THEN 'Mapping'  
					when p.object_type = 'PACKAGE' AND p.procedure_name <> 'MAIN' THEN 'Procedure'
					else p.object_type
			end owb_object_type,
			p.owner object_owner,
			p.object_name,
			p.object_type,
			p.procedure_name,
			topn.sql_id,
			topn.SQL_OPNAME,
			topn.avg_dbtime_per_exec_sec, topn.dbtime_std_dev, topn.min_dbtime_per_exec_sec, topn.max_dbtime_per_exec_sec, topn.numof_executions,
			-- convert CLOB to VARCHAR2 because in some cases I got an ORA-22275: invalid LOB locator specified
			case when s.sql_id is not null then dbms_lob.substr( s.sql_text, 130, 1 ) --s.SQL_TEXT
						else null end sql_text       
		from
		(
			select *
			from (
					select t.user_id, t.PLSQL_ENTRY_OBJECT_ID, t.PLSQL_ENTRY_SUBPROGRAM_ID, t.sql_id, t.SQL_OPNAME, 
							avg(DBtime_secs) avg_dbtime_per_exec_sec, 
							stddev(DBtime_secs) dbtime_std_dev, 
							min(DBtime_secs) min_dbtime_per_exec_sec, 
							max(DBtime_secs) max_dbtime_per_exec_sec, 
							count(distinct t.sql_exec_start||t.sql_id) numof_executions
					from (
						select h_ash.user_id, h_ash.PLSQL_ENTRY_OBJECT_ID, h_ash.PLSQL_ENTRY_SUBPROGRAM_ID, h_ash.sql_id, h_ash.SQL_OPNAME, h_ash.sql_exec_start, sum(10) DBtime_secs
						from dba_hist_active_sess_history h_ash
						where
							h_ash.sql_id = nvl('&sql_id', h_ash.sql_id)
							-- basic filters
							and h_ash.user_id in (3839, 3980) -- ETL_DW, PERIF
							and h_ash.IN_SQL_EXECUTION = 'Y'
							and h_ash.IN_PLSQL_EXECUTION = 'N'
							and h_ash.IS_SQLID_CURRENT = 'Y'
							--and h_ash.PLSQL_ENTRY_OBJECT_ID is not null
							--and h_ash.PLSQL_ENTRY_SUBPROGRAM_ID is not null
							and h_ash.SQL_ID is not null
							--and SQL_OPCODE in (2, 7, 6, 189, 1) -- in ('INSERT', 'DELETE', 'UPDATE', 'UPSERT', 'CREATE TABLE')
							and h_ash.sql_exec_start > sysdate - &days_back  
							AND to_char(h_ash.sql_exec_start, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(h_ash.sql_exec_start, 'DD') end    
							AND trim(to_char(h_ash.sql_exec_start, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(h_ash.sql_exec_start, 'DAY')) end
						GROUP BY h_ash.user_id, h_ash.PLSQL_ENTRY_OBJECT_ID, h_ash.PLSQL_ENTRY_SUBPROGRAM_ID, h_ash.sql_id, h_ash.SQL_OPNAME, h_ash.sql_exec_start
						--order by DBtime_secs desc            
						) t
					group by t.user_id, t.PLSQL_ENTRY_OBJECT_ID, t.PLSQL_ENTRY_SUBPROGRAM_ID, t.sql_id, t.SQL_OPNAME
					order by avg_dbtime_per_exec_sec desc
			) where rownum <= '&n' 
		) topn
			left outer join dba_procedures p on (topn.PLSQL_ENTRY_OBJECT_ID = p.OBJECT_ID AND topn.PLSQL_ENTRY_SUBPROGRAM_ID = p.SUBPROGRAM_ID)
				left outer join DBA_HIST_SQLTEXT s on (topn.sql_id = s.sql_id)
	) fin
	--order by avg_dbtime_per_exec_sec desc
 ) dbt 
	left outer join (
						select REGEXP_REPLACE(REGEXP_REPLACE(execution_name, '_\d\d$', ''),'\w+:' , '' ) exec_name_short, 
							ROUND(avg((updated_on - created_on) * 24 * 60),1) avg_duration_mins,
							ROUND(stddev((updated_on - created_on) * 24 * 60),1) std_dev_duration,
							ROUND( min((updated_on - created_on) * 24 * 60),1) min_duration_mins,
							ROUND( max((updated_on - created_on) * 24 * 60)) max_duration_mins						
						from  owbsys.all_rt_audit_executions 
						WHERE
							CREATED_ON > SYSDATE - (&&days_back)    
							AND to_char(created_on, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
							AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
							AND   execution_audit_status = 'COMPLETE'
							AND return_result = 'OK'  
						GROUP BY REGEXP_REPLACE(REGEXP_REPLACE(execution_name, '_\d\d$', ''),'\w+:' , '' )
	) owb 
	on ( dbt.owb_node_name = owb.exec_name_short )
order by avg_dbtime_per_exec_sec desc 
/		
	