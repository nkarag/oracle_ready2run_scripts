/***********************************

    Get statistics on execution times for the whole tree of a flow
    based on a specific (sysdate - &daysback) historical window
    
    Note:
        This query helps to identify the most time consuming nodes in the tree
        because the first sibling in each family is the one with the largest p80
        (percentile 80) of duration.
        
        %CHECK% nodes are excluded.
        
        If you specify a node_name then the tree will start with root this node_name
        
        If you change the ORDER SIBLINGS BY to CREATED_ON ASC then  you can sort sibling nodes by creation time         

***********************************/

col p80_duration_mins format 999G999D99 
col avg_duration_mins format 999G999D99 
col stddev_duration_mins format 999G999D99 
col max_duration_mins format 999G999D99 
col min_duration_mins format 999G999D99 
col NUMOF_EXECUTIONS format 999
col r format 999
col path format a100 trunc
col execution_name_short format a30
col root_execution_name format a60
col owb_level format 9999999
col owb_type for a15

select  root_execution_name,
        type as owb_type,
        owb_level,
        execution_name_short,        
        p80_duration_mins,
        avg_duration_mins,
        stddev_duration_mins,
        min_duration_mins,
        max_duration_mins,
        numof_executions,
        path        
from (
    -- Query to find the executions per day of the tree        
    with q1 as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
            execution_name,
            task_type,
            created_on,
            updated_on,
            row_number() over(partition by execution_name order by created_on) r,
            PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,
            round(avg((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) avg_duration_mins,
            round(stddev((updated_on - created_on) * 24 * 60) OVER (partition by execution_name) ,1) stddev_duration_mins,
            round(min((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) min_duration_mins,
            round(max((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) max_duration_mins,
            count(distinct TOP_LEVEL_EXECUTION_AUDIT_ID||execution_name) OVER (partition by execution_name) numof_executions        
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
        CONNECT_BY_ROOT execution_name root_execution_name,
        r,
        level,
        lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(execution_name, '_\d\d$', ''),'\w+:' , '' ) execution_name_short,    
        DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)
        TYPE,
        lpad(' ', 2*(level - 1),'   ')||ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
        lpad(' ', 2*(level - 1),'   ')||p80_duration_mins p80_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||avg_duration_mins avg_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||stddev_duration_mins stddev_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||min_duration_mins min_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||max_duration_mins max_duration_mins,
        lpad(' ', 2*(level - 1),'   ')||numof_executions numof_executions,
        --PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,        
        LEVEL owb_level,    
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
       SYS_CONNECT_BY_PATH (
          SUBSTR (execution_name, INSTR (execution_name, ':') + 1),
          '/')
          path  
    from q1 a --, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
    WHERE 
       --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
       --AND CONNECT_BY_ISLEAF = 1   
       --AND CONNECT_BY_ROOT execution_name = b.flow_name
        SUBSTR (execution_name, INSTR (execution_name, ':') + 1) NOT LIKE '%CHECK%' -- exclude "check" nodes
       --AND execution_name like nvl(trim('%&&node_name'),execution_name)
    START WITH --a.PARENT_EXECUTION_AUDIT_ID IS NULL or 
            execution_name like '%'||nvl(trim('&node_name'), trim('&&flow_name'))  
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
    ORDER SIBLINGS BY a.p80_DURATION_MINS DESC --a.created_on, a.p80_DURATION_MINS DESC
) tt
where
   tt.p80_duration_mins > 1
   AND tt.r = 1        
/

undef flow_name
undef critical_ind
undef days_back
undef monthly_only
undef mondays_only
undef node_name