select *
from  
    ( -- CURRENT NODES QUERY
        with q1 as ( 
                    -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
                    select /*+  qb_name(subq_factoring)  materialize dynamic_sampling (4)  */  --   parallel(32) full(a)   
                        execution_audit_id, 
                        parent_execution_audit_id,
                        TOP_LEVEL_EXECUTION_AUDIT_ID,
                        execution_name,
                        task_type,
                        created_on,
                        updated_on,            
                        row_number() over(partition by parent_execution_audit_id order by (updated_on - created_on) desc) lpf_ind, -- longest per family indicator
                        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,
                        round(avg((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) avg_duration_mins,
                        round(stddev((updated_on - created_on) * 24 * 60) OVER (partition by execution_name) ,1) stddev_duration_mins,
                        round(min((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) min_duration_mins,
                        round(max((updated_on - created_on) * 24 * 60) OVER (partition by execution_name),1) max_duration_mins,
                        count(distinct TOP_LEVEL_EXECUTION_AUDIT_ID||execution_name) OVER (partition by execution_name) numof_executions        
                    from  owbsys.all_rt_audit_executions a
                    where
                        TOP_LEVEL_EXECUTION_AUDIT_ID in (  -- select flow of interest and criticality of interest(get the last run (sysdate - 1))
                                                         select /*+  qb_name(qb_sel_flows) leading(dwp_etl_flows) */ execution_audit_id
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
                                                            --AND execution_audit_status = 'COMPLETE'
                                                            --AND return_result = 'OK'                                                        
                                                        )
                        -- restricitons for all the nodes (not just the root) 
                        AND CREATED_ON > SYSDATE - (&&days_back)    
                        AND to_char(created_on, 'DD') = case when nvl('&&monthly_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
                        AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
                        --AND   execution_audit_status = 'COMPLETE'
                        --AND return_result = 'OK'                     
                )                                                                                           
                select /*+ dynamic_sampling (4)  qb_name(owb_hierarch_qry)  */  
                    CONNECT_BY_ROOT a.execution_name root_execution_name,
                    a.lpf_ind,
                    a.created_on,
                    a.updated_on,        
                    --prev.created_on prev_created_on,
                    --prev.updated_on prev_updated_on,                
                    level flow_level,
                    lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(a.execution_name, '_\d\d$', ''),'\w+:' , '' ) execution_name_short,    
                    DECODE (a.task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', a.task_type)      TYPE,
                    --lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(prev.execution_name, '_\d\d$', ''),'\w+:' , '' ) prev_execution_name_short,    
                    --DECODE (prev.task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', prev.task_type)      PREV_TYPE,                
                    lpad(' ', 2*(level - 1),'   ')||ROUND ( (a.updated_on - a.created_on) * 24 * 60, 1) duration_mins,
                    --ROUND ( (prev.updated_on - prev.created_on) * 24 * 60, 1) prev_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.p80_duration_mins p80_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.avg_duration_mins avg_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.stddev_duration_mins stddev_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.min_duration_mins min_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.max_duration_mins max_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.numof_executions numof_executions,
                    --PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,            
                    a.TOP_LEVEL_EXECUTION_AUDIT_ID,
                    CONNECT_BY_ROOT a.execution_audit_id root_execution_audit_id,  
                    a.PARENT_EXECUTION_AUDIT_ID,      
                    a.execution_audit_id,
                    a.execution_name,
                   -- prev.execution_audit_id prev_execution_audit_id,
                   ROUND((CONNECT_BY_ROOT a.updated_on - a.created_on) * 24 * 60, 1) mins_until_end_of_root,
                   --b.critical_ind,
                   ROUND ( (CONNECT_BY_ROOT a.updated_on - CONNECT_BY_ROOT a.created_on) * 24, 1) root_duration_hrs,
                   CONNECT_BY_ROOT a.created_on root_created_on,
                   CONNECT_BY_ROOT a.updated_on root_updated_on,
                   CONNECT_BY_ISLEAF "IsLeaf",
                   SYS_CONNECT_BY_PATH (SUBSTR (a.execution_name, INSTR (a.execution_name, ':') + 1),'/') path         
                from  q1 a               
                WHERE 1=1      
                     --AND a.task_type <> 'AND'
                     --AND a.task_type NOT IN ('AND') AND prev.task_type NOT IN ('AND')  
                    --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
                   --AND CONNECT_BY_ISLEAF = 1   
                   --AND CONNECT_BY_ROOT execution_name = b.flow_name
                    --SUBSTR (execution_name, INSTR (execution_name, ':') + 1) NOT LIKE '%CHECK%' -- exclude "check" nodes
                   --AND execution_name like nvl(trim('%&&node_name'),execution_name)
                START WITH a.execution_name like nvl(upper(trim('%&node_name')), upper(trim('&&flow_name'))) --a.PARENT_EXECUTION_AUDIT_ID IS NULL                
                CONNECT BY  PRIOR a.execution_audit_id = a.parent_execution_audit_id
                ORDER SIBLINGS BY  a.TOP_LEVEL_EXECUTION_AUDIT_ID desc, a.created_on asc, a.p80_DURATION_MINS DESC -- a.p80_DURATION_MINS DESC
    ) hq1
    where
        hq1.flow_level = '&&level'


) curr                     
                
                
                q1 a 
                        left outer join q1 prev
                        on (
                                a.TOP_LEVEL_EXECUTION_AUDIT_ID = prev.TOP_LEVEL_EXECUTION_AUDIT_ID -- same root execution
                                AND a.parent_execution_audit_id = prev.parent_execution_audit_id --same family
                                AND a.created_on between prev.updated_on and (prev.updated_on + 1/24/60/60 )-- sequential execution
                                --AND trunc(a.created_on, 'MI') = trunc(prev.updated_on, 'MI') -- sequential execution
                                AND a.execution_audit_id <> prev.execution_audit_id -- avoid node selfjoin 
                           )  