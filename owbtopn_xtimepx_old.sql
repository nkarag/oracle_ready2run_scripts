col avg_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col std_dev_duration format 999G999G999D99 justify right null '<NULL>' 
col max_duration_mins format 999G999G999D99 justify right null '<NULL>' 
col min_duration_mins format 999G999G999D99 justify right null '<NULL>'
col NUMOF_EXECUTIONS format 999G999
col r format 999
col owb_node_name format a30
col OWB_OBJECT_TYPE format a20
col main_flow format a30
col path format a200

undef flow_name
undef critical_ind
undef days_back
undef monthly_only
undef mondays_only


/*
    Find the top N heaviest mappings in the last"days_back" days from a specific date (execution_date) for a specific top-level flow  
    (exclude "%CHECK%" nodes)
    
    INPUT
        * flow_name: name of the flow of interest (if null, it runs for all top level flows)
        * critical_ind:  0,1 (indication if flow is critical or no, according to table monitor_dw.dwp_etl_flows)
        * execution_date: date milestone from which we want to check X days back (if left  null then it uses sysdate),
                         in the form dd/mm/yyyy
        * days_back: how many days back you want to check execution history
        * N : define the "N" for top N

    Example
        Show me the top 10 heaviest mappings of SOC_DW_MAIN for all executions 30 days back from 12/03/2014
        
        flow_name:  SOC_DW_MAIN
        critical_ind: 1
        execution_date: 12/03/2014
        days_back: 30
        N: 10          
*/

    select	rownum r,
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
				START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
				CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
				 --ORDER BY  duration_mins DESC--root_execution_name, root_execution_audit_id DESC, duration_mins DESC
		 ) agg
		 GROUP BY	root_execution_name,
					execution_name_short,
					TYPE,
					"Path"
		 ORDER BY avg_duration_mins desc
    ) tt
    where
        rownum <= &N			
/		
