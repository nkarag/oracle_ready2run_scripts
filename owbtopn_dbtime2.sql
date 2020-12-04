col username format a8
col sql_opname format a15
col sql_text format a130 trunc
col object_type format a11
col OBJECT_OWNER format a11
col procedure_name format a30
col object_name format a30
col owb_node_name format a30
col OWB_OBJECT_TYPE format a20
col main_flow format a20
col PROCEDURE_NAME format a30
col DBtime_secs format 999G999G999D99
col avg_dbtime_per_exec_sec format 999G999G999G999
col min_dbtime_per_exec_sec format 999G999G999G999
col max_dbtime_per_exec_sec format 999G999G999G999
col NUMOF_EXECUTIONS format 999G999
col avg_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col max_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col min_duration_mins format 999G999G999D99 justify right null '<NULL>'
col SQL_ID format a16 justify center
col r format 999

undef days_back
undef monthly_only
undef mondays_only


select
		row_number() over(order by avg_dbtime_per_exec_sec desc ) r,
		dbt.avg_dbtime_per_exec_sec, dbt.min_dbtime_per_exec_sec, dbt.max_dbtime_per_exec_sec, dbt.numof_executions,
		avg_duration_mins,
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
			fin.avg_dbtime_per_exec_sec, fin.min_dbtime_per_exec_sec, fin.max_dbtime_per_exec_sec, fin.numof_executions,
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
					else 'UFO'
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
			topn.avg_dbtime_per_exec_sec, topn.min_dbtime_per_exec_sec, topn.max_dbtime_per_exec_sec, topn.numof_executions,
			-- convert CLOB to VARCHAR2 because in some cases I got an ORA-22275: invalid LOB locator specified
			case when s.sql_id is not null then dbms_lob.substr( s.sql_text, 130, 1 ) --s.SQL_TEXT
						else null end sql_text       
		from
		(
			select *
			from (
					select t.user_id, t.PLSQL_ENTRY_OBJECT_ID, t.PLSQL_ENTRY_SUBPROGRAM_ID, t.sql_id, t.SQL_OPNAME, 
							avg(DBtime_secs) avg_dbtime_per_exec_sec, min(DBtime_secs) min_dbtime_per_exec_sec, max(DBtime_secs) max_dbtime_per_exec_sec, count(distinct sql_exec_start) numof_executions
					from (
						select h_ash.user_id, h_ash.PLSQL_ENTRY_OBJECT_ID, h_ash.PLSQL_ENTRY_SUBPROGRAM_ID, h_ash.sql_id, h_ash.SQL_OPNAME, h_ash.sql_exec_start, sum(10) DBtime_secs
						from dba_hist_active_sess_history h_ash
						where 
							-- basic filters
							h_ash.user_id in (3839, 3980) -- ETL_DW, PERIF
							and h_ash.IN_SQL_EXECUTION = 'Y'
							and h_ash.IN_PLSQL_EXECUTION = 'N'
							and h_ash.IS_SQLID_CURRENT = 'Y'
							and h_ash.PLSQL_ENTRY_OBJECT_ID is not null
							and h_ash.PLSQL_ENTRY_SUBPROGRAM_ID is not null
							and h_ash.SQL_ID is not null
							--and SQL_OPCODE in (2, 7, 6, 189, 1) -- in ('INSERT', 'DELETE', 'UPDATE', 'UPSERT', 'CREATE TABLE')
							and h_ash.sample_time > sysdate - &days_back  
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
	