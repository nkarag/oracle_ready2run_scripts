col p80_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col avg_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col std_dev_duration format 999G999G999D99 justify right null '<NULL>' 
col max_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col min_duration_mins format 999G999G999D99 justify right null '<NULL>'
col NUMOF_EXECUTIONS format 999G999
col r format 999
col owb_node_name format a30
col OWB_OBJECT_TYPE format a20
col main_flow format a30
col path format a100

col username format a8
col sql_opname format a15
col sql_text format a130 trunc


undef flow_name
undef critical_ind
undef days_back
undef monthly_only
undef mondays_only
undef node_name


/*
Find top N OWB nodes with the most **average** (wall-clock) execution time. You can also select the mappings of a specific Main Flow only, or
only for critical or non-critical flows. It then tries to join through DBA_PROCEDURES to DBA_HIST_ACTIVE_SESS_HISTORY in order to get the 
corresponding SQL_ID.

--	Note:
--		Instead of the average DB time, we currently use the Percentile 80, to find the "heaviest one" as more accurate metric than the average
    
    INPUT
        * flow_name: name of the flow of interest (if null, it runs for all top level flows)
        * critical_ind:  0,1 (indication if flow is critical or no, according to table monitor_dw.dwp_etl_flows)
        * days_back: how many days back from sysdate you want to check execution history
        * N : define the "N" for top N
		* node_name:  (optional) you can get the results for a specific node only

*/

select /*+ parallel(32) */
	row_number() over(order by p80_duration_mins desc) r, --avg_duration_mins desc ) r,
	owb.p80_duration_mins,
--	owb.p80_disc,
	owb.avg_duration_mins,
	owb.std_dev_duration,
	owb.min_duration_mins,
	owb.max_duration_mins,
	owb.numof_executions,
	ash_final.username,
	owb.main_flow,
	owb.owb_node_name,
	owb.OWB_OBJECT_TYPE,
	owb.path,	
	ash_final.sql_id,
	ash_final.LAST_SQL_PLAN_HASH_VALUE,
	ash_final.SQL_OPNAME,
	ash_final.last_exec_start,
	ash_final.last_sample_time,
	-- convert CLOB to VARCHAR2 because in some cases I got an ORA-22275: invalid LOB locator specified
	ash_final.sql_text
from (	-- OWB
    select	--rownum r,
			tt.p80_duration_mins,
--			tt.p80_disc,
			tt.avg_duration_mins,
			tt.std_dev_duration,
			tt.min_duration_mins,
			tt.max_duration_mins,
			tt.numof_executions,
			tt.main_flow,
			tt.owb_node_name,
			tt.OWB_OBJECT_TYPE,
			tt.path
    from (
			select
				PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY duration_mins ASC) p80_duration_mins,
--				PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY duration_mins ASC) p80_disc,
				avg(duration_mins) avg_duration_mins,
				stddev(duration_mins) std_dev_duration,
				min(duration_mins) min_duration_mins,
				max(duration_mins) max_duration_mins,
				count(distinct root_execution_audit_id||execution_name_short) numof_executions,
				root_execution_name main_flow,
				execution_name_short owb_node_name,
				TYPE OWB_OBJECT_TYPE,
				"Path" path								
			from (
					-- Query to find the execution of all mappings or procedures and functions for each parent ETL Flow of interest
					--and criticality of interest and history of interest
				with q1 as ( 
					-- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
					select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
						execution_name,
						task_type,
						created_on,
						updated_on
					from  owbsys.all_rt_audit_executions a
					where
						TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flow of interest and criticality of interest(get the last run (sysdate - 1))
														 select  execution_audit_id
														 from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
														 where 
															execution_name = flow_name
															and PARENT_EXECUTION_AUDIT_ID IS NULL
															AND flow_name = nvl('&&flow_name',flow_name)
															AND critical_ind = nvl('&&critical_ind',critical_ind)
															-- restrictions for the main flow
															AND CREATED_ON > SYSDATE - (&&days_back)    
															AND to_char(created_on, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
															AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
															AND execution_audit_status = 'COMPLETE'
															AND return_result = 'OK'                                                        
														)
						-- restricitons for all the nodes (not just the root) 
						AND CREATED_ON > SYSDATE - (&&days_back)    
						AND to_char(created_on, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
						AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
						AND   execution_audit_status = 'COMPLETE'
						AND return_result = 'OK' 					
				)                                                                                           
				select /*+ dynamic_sampling (4) */  
					ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,	
					CONNECT_BY_ROOT execution_name root_execution_name,
					REGEXP_REPLACE(REGEXP_REPLACE(execution_name, '_\d\d$', ''),'\w+:' , '' ) execution_name_short,	
					DECODE (task_type,
						   'PLSQL', 'Mapping',
						   'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
					TYPE,
					CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,		
					execution_audit_id,
					execution_name,
				   created_on,
				   updated_on,
				   ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) mins_until_end_of_root,
				   --b.critical_ind,
				   ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
				   CONNECT_BY_ROOT created_on root_created_on,
				   CONNECT_BY_ROOT updated_on root_updated_on,
				   CONNECT_BY_ISLEAF "IsLeaf",
				   LEVEL,
				   SYS_CONNECT_BY_PATH (
					  SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
					  '/')
					  "Path"  
				from q1 a--, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
				WHERE 
				   task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
				   AND CONNECT_BY_ISLEAF = 1   
				   --AND CONNECT_BY_ROOT execution_name = b.flow_name
				   AND SUBSTR (execution_name, INSTR (execution_name, ':') + 1) NOT LIKE '%CHECK%' -- exclude "check" nodes
				   AND execution_name like nvl(trim('%&node_name'),execution_name)
				START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
				CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
				 --ORDER BY  duration_mins DESC--root_execution_name, root_execution_audit_id DESC, duration_mins DESC
		 ) agg
		 GROUP BY	root_execution_name,
					execution_name_short,
					TYPE,
					"Path"
		 ORDER BY p80_duration_mins desc --avg_duration_mins desc
    ) tt
    where
        rownum <= &N
) owb
	left outer join
		(
			select
				ash.username,
				ash.sql_id,
				ash.LAST_SQL_PLAN_HASH_VALUE,
				p.object_name,
				case 	when p.object_type = 'PACKAGE' AND p.procedure_name = 'MAIN' THEN 'Mapping'  
						when p.object_type = 'PACKAGE' AND p.procedure_name <> 'MAIN' THEN 'Procedure'
						when p.object_type = 'PROCEDURE' THEN 'Procedure'
						when p.object_type = 'FUNCTION' THEN 'Function'
						else p.object_type
				end owb_object_type,
				p.object_type,
				p.procedure_name,
				ash.SQL_OPNAME,
				ash.last_exec_start,
				ash.last_sample_time,
				ash.sql_text,
				ash.PLSQL_ENTRY_OBJECT_ID,
				ash.PLSQL_ENTRY_SUBPROGRAM_ID				
			from
			(	-- ASH
				select 
					decode(USER_ID, 3839, 'ETL_DW', 3980, 'PERIF') username,
					t3.SQL_ID,
					t3.SQL_PLAN_HASH_VALUE LAST_SQL_PLAN_HASH_VALUE,
					--(select object_name from dba_procedures where object_id = t3.PLSQL_ENTRY_OBJECT_ID and subprogram_id = t3.PLSQL_ENTRY_SUBPROGRAM_ID) object_name,
					--(select object_type from dba_procedures where object_id = t3.PLSQL_ENTRY_OBJECT_ID and subprogram_id = t3.PLSQL_ENTRY_SUBPROGRAM_ID) object_type,
					--(select procedure_name from dba_procedures where object_id = t3.PLSQL_ENTRY_OBJECT_ID and subprogram_id = t3.PLSQL_ENTRY_SUBPROGRAM_ID) procedure_name,
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
					select -- for each mapping/procedure find the sql_id with the most samples in ASH (or most dbtime)
						row_number() over(partition by PLSQL_ENTRY_OBJECT_ID, PLSQL_ENTRY_SUBPROGRAM_ID order by dbtime desc, sample_time desc) r,
						t2.*
					from (	
						select 
							sum(10) over(partition by T.PLSQL_ENTRY_OBJECT_ID, T.PLSQL_ENTRY_SUBPROGRAM_ID, SQL_ID) dbtime,
							t.*
						from dba_hist_active_sess_history t
						where 
							-- basic filters
							user_id in (3839, 3980) -- ETL_DW, PERIF
							and T.IN_SQL_EXECUTION = 'Y'
							and T.IN_PLSQL_EXECUTION = 'N'
							and T.IS_SQLID_CURRENT = 'Y'
							and T.PLSQL_ENTRY_OBJECT_ID is not null
							and T.PLSQL_ENTRY_SUBPROGRAM_ID is not null
							and SQL_ID is not null
							--and SQL_OPCODE in (2, 7, 6, 189, 1) -- in ('INSERT', 'DELETE', 'UPDATE', 'UPSERT', 'CREATE TABLE')
							and t.sql_exec_start > sysdate - &&days_back
							AND to_char(t.sql_exec_start, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(t.sql_exec_start, 'DD') end    
							AND trim(to_char(t.sql_exec_start, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(t.sql_exec_start, 'DAY')) end
					)t2        
				)t3, DBA_HIST_SQLTEXT s
				where
					r=1
					and t3.sql_id = s.sql_id (+)							
			) ash
				join
					dba_procedures p
				on (p.object_id = ash.PLSQL_ENTRY_OBJECT_ID and p.subprogram_id = ash.PLSQL_ENTRY_SUBPROGRAM_ID) --and p.owner = ash.username )
		) ash_final
	on (owb.owb_node_name = ash_final.object_name AND owb.owb_object_type = ash_final.owb_object_type)
order by owb.p80_duration_mins desc --owb.avg_duration_mins desc	
/		
