
-- 1. create a temp table from the statement in question
create table temptbl as
with q1 as ( 
    -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
    select /*+ materialize dynamic_sampling (4) */  
        execution_audit_id, 
        parent_execution_audit_id,
        top_level_execution_audit_id,
        execution_name,
        task_type,
        created_on,
        updated_on,
        execution_audit_status,
        return_result
    from  owbsys.all_rt_audit_executions a
    where
        TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flows of interest and criticallity of interest
                                         select execution_audit_id
                                         from owbsys.all_rt_audit_executions, monitor_dw.dwp_etl_flows
                                         where 
                                            execution_name = flow_name
                                            and PARENT_EXECUTION_AUDIT_ID IS NULL
                                            AND flow_name = nvl('&&flow_name',flow_name)
                                            AND critical_ind = nvl('&&critical_ind',critical_ind)
                                            -- restrictions for the main flow
                                            --and created_on > sysdate - &&days_back
                                            and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                                            --AND execution_audit_status = 'COMPLETE'
                                            --AND return_result = 'OK'                                                        
                                        )
    -- restricitons for all the nodes (not just the root)    
    --AND CREATED_ON > SYSDATE - (&days_back)
    and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1    
     AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
     AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
   --AND a.execution_audit_status = 'COMPLETE'
   --AND a.return_result = 'OK'
),
--q2 as 
--( -- query to get mapping  execution details
--    select /*+ materialize */ MAP_NAME, MAP_TYPE, START_TIME, END_TIME, round(ELAPSE_TIME/60,1) elapsed_time_mins, RUN_STATUS, SOURCE_LIST, TARGET_LIST, NUMBER_ERRORS, NUMBER_RECORDS_SELECTED, NUMBER_RECORDS_INSERTED, NUMBER_RECORDS_UPDATED, NUMBER_RECORDS_DELETED, NUMBER_RECORDS_MERGED 
--    from OWBSYS.ALL_RT_AUDIT_MAP_RUNS 
--    where 1=1
--     --start_time > SYSDATE - (&&days_back)
--     and start_time between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1    
--     AND to_char(start_time, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(start_time, 'DD') end    
--     AND trim(to_char(start_time, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(start_time, 'DAY')) end
--),
q3 as
( -- query to get from AUDIT_DETAILS the exetuion time of the mapping (i.e., not including pre/post session procedures within the node)
    select /*+ materialize leading(t2 t1) */ T2.RTE_ID, --T3.EXECUTION_NAME, T3.CREATED_ON, T3.UPDATED_ON, 
            round(T1.RTD_ELAPSE/60,1) MINS_MAP_ONLY_SINGLE_TARGET, 
            round(T2.RTA_ELAPSE/60,1) MINS_MAP_ONLY_ALL_TARGETS,
            min(T1.CREATION_DATE) over(partition by T2.RTE_ID ) mapping_created_on_min, 
            max(T1.LAST_UPDATE_DATE) over(partition by T2.RTE_ID) mapping_updated_on_max,
            --ROUND ( (t3.updated_on - t3.created_on) * 24 * 60, 1) - round(T1.RTD_ELAPSE/60,1) MINS_IN_PRE_POST_MAPPING,
            T2.RTA_PRIMARY_SOURCE,
            T1.RTD_TARGET,
            T1.RTD_NAME,
            T1.RTD_SELECT, T1.RTD_INSERT, T1.RTD_UPDATE, T1.RTD_DELETE, T1.RTD_MERGE       
    from OWBSYS.OWB$WB_RT_AUDIT_DETAIL t1, OWBSYS.OWB$WB_RT_AUDIT t2--, q1 t3
    where 1=1
        AND T1.RTA_IID = T2.RTA_IID
        -- impose the time constraint also in here so as to limit the number of rows
        and T2.RTA_DATE between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1    
        AND to_char(T2.RTA_DATE, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(T2.RTA_DATE, 'DD') end    
        AND trim(to_char(T2.RTA_DATE, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(T2.RTA_DATE, 'DAY')) end       
        --AND t3.execution_name like trim('%&&node_name')
        --AND T2.RTA_LOB_NAME like '%SOC_SAVEDESK_FCT_UPD%'
        --AND T2.RTE_ID = T3.EXECUTION_AUDIT_ID
        --AND T3.EXECUTION_NAME like '%SOC_SAVEDESK_FCT_UPD'
    --order by T1.CREATION_DATE desc
)                                                                                          
select /*+ dynamic_sampling (4) gather_plan_statistics */  
   CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
   execution_audit_id,
   SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1) execution_name_short,
   DECODE (task_type,
           'PLSQL', 'Mapping',
           'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)    TYPE,
   q1.created_on,
   q1.updated_on, execution_audit_status,  return_result,
   ROUND ( (q1.updated_on - q1.created_on) * 24 * 60, 1) duration_mins,
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (q1.updated_on - q1.created_on) * 24 * 60, 1) ASC) OVER (partition by SYS_CONNECT_BY_PATH (
                                                                                    SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),'/')/*execution_name*/) duration_mins_p80,
   q3.MINS_MAP_ONLY_ALL_TARGETS,
   ROUND ( (q1.updated_on - q1.created_on) * 24 * 60 /*duration_mins*/ - q3.MINS_MAP_ONLY_ALL_TARGETS, 1)   MINS_IN_PRE_AND_POST_MAP,   
   q3.MINS_MAP_ONLY_SINGLE_TARGET,
   q3.RTD_TARGET,
   q3.RTD_NAME,        
   ROUND((q3.mapping_created_on_min - q1.created_on)* 24 * 60, 1) MINS_IN_PREMAPPING,
   ROUND((q1.updated_on - q3.mapping_updated_on_max)* 24 * 60, 1) MINS_IN_POSTMAPPING,                                                                                     
   ROUND((
            (   select c.updated_on 
                from owbsys.all_rt_audit_executions c 
                where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                      AND  c.execution_name like trim('%'||nvl('&&mstone_node_name','xxxxxx'))
            ) 
        - q1.created_on) * 24 , 1) hr_unt_end_of_mstone_incl_node,
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((
            (   select c.updated_on 
                from owbsys.all_rt_audit_executions c 
                where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                      AND  c.execution_name like trim('%'||nvl('&mstone_node_name','xxxxxx'))
            ) 
        - q1.created_on) * 24 , 1)  ASC) OVER (partition by SYS_CONNECT_BY_PATH (
                                                                                    SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),'/') /*execution_name*/) hrs_unt_end_of_mstone_p80,        
   ROUND((CONNECT_BY_ROOT q1.updated_on - q1.created_on) * 24 , 1) hr_until_end_of_root_incl_node,   
   PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((CONNECT_BY_ROOT q1.updated_on - q1.created_on) * 24 , 1)  ASC) OVER (partition by SYS_CONNECT_BY_PATH (
                                                                                    SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),'/') /*execution_name*/) hrs_until_end_of_root_p80,      
   CONNECT_BY_ROOT q1.execution_name root_execution_name,
   --b.critical_ind,
   ROUND ( (CONNECT_BY_ROOT q1.updated_on - CONNECT_BY_ROOT q1.created_on) * 24, 1) root_duration_hrs,
   CONNECT_BY_ROOT q1.created_on root_created_on,
   CONNECT_BY_ROOT q1.updated_on root_updated_on,
   CONNECT_BY_ISLEAF "IsLeaf",
   LEVEL node_level,
   SYS_CONNECT_BY_PATH (
      SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1),      '/') "Path" ,
    q3.RTA_PRIMARY_SOURCE,
    --q3.RTD_TARGET,
    q3.RTD_SELECT, q3.RTD_INSERT, q3.RTD_UPDATE, q3.RTD_DELETE, q3.RTD_MERGE   
--   q2.* 
from q1 --, (select * from monitor_dw.dwp_etl_flows where critical_ind = nvl('&critical_ind',critical_ind) AND flow_name = nvl('&flow_name',flow_name)) b
--    LEFT OUTER JOIN q2
--        ON (SUBSTR (q1.execution_name, INSTR (q1.execution_name, ':') + 1) = q2.map_name 
--            AND q2.start_time between q1.created_on AND q1.updated_on)
        LEFT OUTER JOIN q3
            ON (q1.execution_audit_id = q3.RTE_ID)                    
WHERE 1=1 
   --AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
   AND q1.execution_name like trim('%&&node_name')
   --AND CONNECT_BY_ISLEAF = 1 
   --AND execution_audit_status = 'COMPLETE'
   --AND return_result = 'OK'  
   --------AND CONNECT_BY_ROOT execution_name = b.flow_name
START WITH PARENT_EXECUTION_AUDIT_ID IS NULL
CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
ORDER BY  root_execution_name, root_execution_audit_id DESC, execution_audit_id desc --, duration_mins DESC;


with coldefs
as (
    -- get column definitions of the temp table
    select *
    from dba_tab_Columns
    where
        table_name = upper('temptbl')
        and owner = 'NKARAG'
    order by column_id
)
select  'col '|| column_name ||' format '|| case    when data_type = 'NUMBER' then rpad('9',data_length, '9')
                                                    when data_type = 'VARCHAR2' then 'a'||data_length
                                                    when data_type = 'DATE' then 'a18'
                                                    else ''
                                            end frmt                                                                                                        
from coldefs;         


-- drop temp table
drop table nkarag.temptbl;