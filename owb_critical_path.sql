------------------------------------------------------------------------------------------
-- owb_critical_path.sql
--
--  Find the Critical Path(CP) of OWB flow
--
-- Parameters
--  flow_name                   Specify the name of the top-level flow
--  root_node_name (optional)   Name of a sub-flow node. Specify this parameter if you want
--                              to get the CP of a subflow other than the top flow, otherwise
--                              leave null.
--  days_back                   Num of days back from sysdate of required history
--  monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
--  Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays  
--
-- (C) 2014 Nikos Karagiannidis http://oradwstories.blogspot.com          
------------------------------------------------------------------------------------------

/*

When are two nodes n1, n2 of the same parent sequential?
    when:  n1.updated_on = n2.created_on

When are two nodes n1, n2 of the same parent parallel?
    when:  n1.created_on = n2.created_on
    
How to identify the Critical Path in a family (same parent) of nodes?    
    Find all the distinct sequenses of nodes and from these choose the longest sequence, this is the Critical Path
*/


CREATE OR REPLACE FUNCTION EVAL(EXPR VARCHAR2) RETURN NUMBER PARALLEL_ENABLE IS 
   RC NUMBER;
BEGIN
   EXECUTE IMMEDIATE 'BEGIN :1 := '||EXPR||'; END;' USING OUT RC;
   RETURN RC;
END;
/



--  Aggregation along the path
-- http://www.remote-dba.net/t_advanced_sql_aggregation_hierarchies.htm
-- https://asktom.oracle.com/pls/asktom/f?p=100:11:::::P11_QUESTION_ID:30609389052804
-- google:  oracle connect by path aggregation
-- sql_id 3duthdyfygsq9   a4gmdb7z7wvu5 78gg9tvsrnkas

-- http://jonathanlewis.wordpress.com/2007/06/25/qb_name/

col ROOT_EXECUTION_NAME format a30
col PATH_DURATION_MINS format 999G999D99 
col CRITICAL_PATH format a10000
col SEQ_LENGTH format 999

undef flow_name
undef root_node_name
undef critical_ind
undef days_back
undef monthly_only
undef mondays_only

select *
from (
select  /*+ qb_name(main) leading(@qb_sel_flows @subq_factoring @owb_hierarch_qry    ) */
            row_number() over(partition by  root_created_on order by  path_duration_mins desc, seq_length desc) r,    
            root_execution_name, 
            root_created_on, 
            root_updated_on, 
         --   flow_level, 
            PATH_DURATION_MINS,   
            seq_length,         
            SEQ_PATH CRITICAL_PATH 
from (
    select /*+ qb_name(T4_block) */  root_execution_name, 
            root_created_on, 
            root_updated_on, 
          --  flow_level, 
            SEQ_PATH, seq_length, 
            max(seq_length) over(partition by root_created_on) maxlength,
            PATH_DURATION_MINS,
            max(PATH_DURATION_MINS) over(partition by root_created_on) max_path_dur_mins                        
    from (
        select /*+ qb_name(seq_path_qry) */ CONNECT_BY_ISLEAF "IsLeaf",
               LEVEL seq_length, SYS_CONNECT_BY_PATH(execution_name_short||' ['||trim(to_char(duration_mins))||', p80-'||trim(to_char(p80_duration_mins))||', TYPE:'||type||']', ' -->') SEQ_PATH,
               EVAL(SYS_CONNECT_BY_PATH(NVL(TO_CHAR(duration_mins),'NULL'),'+')) PATH_DURATION_MINS,               
               t2.duration_mins + prior t2.duration_mins,
               T2.*
        from (
            select /*+ qb_name(T2_block) parallel(32) */ count(*) over(partition by t1.execution_audit_id ) cnt,  -- find nodes with more than one "previous nodes"
                    t1.*
            from (
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
                    prev.created_on prev_created_on,
                    prev.updated_on prev_updated_on,                
                    level flow_level,
                    lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(a.execution_name, '_\d\d$', ''),'\w+:' , '' ) execution_name_short,    
                    DECODE (a.task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', a.task_type)      TYPE,
                    lpad(' ', 2*(level - 1))||REGEXP_REPLACE(REGEXP_REPLACE(prev.execution_name, '_\d\d$', ''),'\w+:' , '' ) prev_execution_name_short,    
                    DECODE (prev.task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', prev.task_type)      PREV_TYPE,                
                    lpad(' ', 2*(level - 1),'   ')||ROUND ( (a.updated_on - a.created_on) * 24 * 60, 1) duration_mins,
                    ROUND ( (prev.updated_on - prev.created_on) * 24 * 60, 1) prev_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.p80_duration_mins p80_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.avg_duration_mins avg_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.stddev_duration_mins stddev_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.min_duration_mins min_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.max_duration_mins max_duration_mins,
                    lpad(' ', 2*(level - 1),'   ')||a.numof_executions numof_executions,
                    --PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((updated_on - created_on) * 24 * 60, 1)  ASC) OVER (partition by execution_name) p80_duration_mins,            
                    a.TOP_LEVEL_EXECUTION_AUDIT_ID,
                    CONNECT_BY_ROOT a.execution_audit_id root_execution_audit_id,        
                    a.execution_audit_id,
                    a.execution_name,
                    prev.execution_audit_id prev_execution_audit_id,
                   ROUND((CONNECT_BY_ROOT a.updated_on - a.created_on) * 24 * 60, 1) mins_until_end_of_root,
                   --b.critical_ind,
                   ROUND ( (CONNECT_BY_ROOT a.updated_on - CONNECT_BY_ROOT a.created_on) * 24, 1) root_duration_hrs,
                   CONNECT_BY_ROOT a.created_on root_created_on,
                   CONNECT_BY_ROOT a.updated_on root_updated_on,
                   CONNECT_BY_ISLEAF "IsLeaf",
                   SYS_CONNECT_BY_PATH (SUBSTR (a.execution_name, INSTR (a.execution_name, ':') + 1),'/') path         
                from q1 a 
                        left outer join q1 prev
                        on (
                                a.TOP_LEVEL_EXECUTION_AUDIT_ID = prev.TOP_LEVEL_EXECUTION_AUDIT_ID -- same root execution
                                AND a.parent_execution_audit_id = prev.parent_execution_audit_id --same family
                                AND a.created_on between prev.updated_on and (prev.updated_on + 2/24/60/60 )-- sequential execution
                                --AND trunc(a.created_on, 'MI') = trunc(prev.updated_on, 'MI') -- sequential execution
                                AND a.execution_audit_id <> prev.execution_audit_id -- avoid node selfjoin 
                           )                  
                WHERE 1=1      
                     --AND a.task_type <> 'AND'
                     --AND a.task_type NOT IN ('AND') AND prev.task_type NOT IN ('AND')  
                    --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
                   --AND CONNECT_BY_ISLEAF = 1   
                   --AND CONNECT_BY_ROOT execution_name = b.flow_name
                    --SUBSTR (execution_name, INSTR (execution_name, ':') + 1) NOT LIKE '%CHECK%' -- exclude "check" nodes
                START WITH  a.execution_name like '%'|| nvl(trim('&root_node_name'), trim('&&flow_name'))  --a.PARENT_EXECUTION_AUDIT_ID IS NULL              
                CONNECT BY  PRIOR a.execution_audit_id = a.parent_execution_audit_id
                ORDER SIBLINGS BY  a.TOP_LEVEL_EXECUTION_AUDIT_ID desc, a.created_on asc, a.p80_DURATION_MINS DESC -- a.p80_DURATION_MINS DESC
            ) t1
            where 
                -- filter on level 2 to get the cpath under the root node
                t1.flow_level = 2              
        ) t2
        where      
            -- exclude AND nodes as "previous nodes" in cases where 2 or more "previous nodes" exist
            not regexp_like( t2.prev_execution_name_short,  case when t2.cnt > 1 then '^\s*AND(\d|_)+.*' ELSE 'xoxoxo' end)
            --regexp_like (t2.prev_execution_name_short,  case when t2.cnt > 1 then '^AND\d+{0}?' ELSE '.*' end)
            -- exclude OR nodes as "previous nodes" in cases where 2 or more "previous nodes" exist
            AND not regexp_like( t2.prev_execution_name_short,  case when t2.cnt > 1 then '^\s*OR(\d|_)+.*' ELSE 'xoxoxo' end)                         
        start with t2.prev_created_on is null
        connect by NOCYCLE  t2.prev_execution_audit_id = prior t2.execution_audit_id
    )t3
    order by root_created_on desc, seq_length desc
    ) t4        
where 1=1
    AND max_path_dur_mins = path_duration_mins
    --AND seq_length = maxlength -- relax this restriction because it is not true always and thus it prunes flow executions (missing days from the result)        
)
where
    r = 1    
order by root_created_on desc, path_duration_mins desc, seq_length desc    
/    

undef flow_name
undef root_node_name
undef critical_ind
undef days_back
undef monthly_only
undef mondays_only


/*
----------------------------------------------------
-- tuning
-------------------------------------------------
execute dbms_sqltune.accept_sql_profile(task_name => -
            '3duthdyfygsq9_c_path', task_owner => 'NKARAG', replace => TRUE);
            

drop index  OWBSYS.IDX$$_146140001        
            
create index OWBSYS.IDX$$_146140001 on
    OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS("WORKSPACE_ID") compute statistics;
    
    
drop index OWBSYS.IDX$$_146140002 
    
create index OWBSYS.IDX$$_146140002 on
    OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS("PARENT_AUDIT_EXECUTION_ID","CREATION_DATE") compute statistics;
    
-------- DRAFT ----------------------                


select sid, serial#, inst_id
from gv$session
where username = USER and (sid) = (select sid from v$mystat where rownum = 1)

select *
from gv$active_session_history
where
    session_id = 1659
order by sample_time desc  


select task_type, count(*)
from owbsys.all_rt_audit_executions
where 
CREATED_ON > SYSDATE - (&&days_back)  
and top_level_execution_audit_id in (37621201, 37694651, 37549891)
group by task_type
order by 2 desc


select task_type, count(*)
from owbsys.all_rt_audit_executions
where 
CREATED_ON > SYSDATE - (&&days_back)
group by task_type
order by 2 desc

select * 
from owbsys.all_rt_audit_executions a
where
a.execution_name like '%' ||nvl(upper(trim('&node_name')), upper(trim('&&flow_name'))) 
and CREATED_ON > SYSDATE - (&&days_back)

*/