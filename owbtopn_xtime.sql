undef execution_date
undef flow_name
undef critical_ind
undef days_back


/*
    Find the top N heaviest mappings (based on wall-clock execution time) in the last "days_back" days from a specific date (execution_date) for a specific top-level flow  
    (exclude "%CHECK%" nodes)
	
	NOTE: if a mapping is everyday among the "heaviest" ones, it will appear multiple times in the result. Thus "top N" for this query does not mean we will get N DISTINCT
		  mappings.
    
    INPUT
        * flow_name: name of the flow of interest (if null, it runs for all top level flows)
        * critical_ind:  0,1 (indication if flow is critical or no, according to table monitor_dw.dwp_etl_flows)
        * execution_date: 	date milestone from which we want to check X days back (if left  null then it uses sysdate),
							in the form dd/mm/yyyy
        * days_back: 	how many days back you want to check execution history
        * N : define the "N" for top N

    Example
        Show me the top 10 heaviest mappings of SOC_DW_MAIN for all executions 30 days back from 12/03/2014
        
        flow_name:  SOC_DW_MAIN
        critical_ind: 1
        execution_date: 12/03/2014
        days_back: 30
        N: 10          
*/

col r format 999
col owb_node_name format a30
col OWB_OBJECT_TYPE format a20
col main_flow format a30
col path format a100

    select rownum r, tt.*
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
                                                and trunc(created_on) > nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) - &&days_back --created_on > sysdate - 1                                                
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                        
                                            )
        -- restricitons for all the nodes (not just the root)    
        --AND trunc(created_on) = nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) --CREATED_ON > SYSDATE - 1
       and trunc(created_on) > nvl(to_date('&&execution_date', 'dd/mm/yyyy'), trunc(sysdate)) - &days_back 
       AND a.execution_audit_status = 'COMPLETE'
       AND a.return_result = 'OK'
    )                                                                                           
    select /*+ dynamic_sampling (4) */  
		ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,	
		CONNECT_BY_ROOT execution_name main_flow,
		SUBSTR (execution_name, INSTR (execution_name, ':') + 1) owb_node_name,	
		DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
        OWB_OBJECT_TYPE,
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
     ORDER BY  duration_mins DESC--root_execution_name, root_execution_audit_id DESC, duration_mins DESC
    ) tt
    where
        rownum <= &N
/		
