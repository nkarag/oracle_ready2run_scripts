/****************
    Query to find the execution of all major flows in the history of interest
    
  Parameters:
  flow_name    (optional) The Main flow name e.g. SOC_DW_MAIN
  critical_ind  (optional)  0/1 if the main flow is critical
  days_back     (optional)  Num of days back from sysdate of required history
  monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
  Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays    
    
*****************/

select *
from (
    select 
      created_on,
       updated_on,
       ROUND ( (updated_on - created_on) * 24 , 1) duration_hrs_total,
       PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - created_on) * 24 , 1) ASC) OVER (partition by execution_name) duration_hrs_total_p80,       
       case when execution_name = 'KPIDW_MAIN'
                THEN  round( ( updated_on - (  select updated_on 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like 'KPIDW_MAIN:CHECK_FLOWSEND_FORGLOBAL_PROC'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'
                                            ) 
                              )*24  -- hours
                      ,1)
            when execution_name = 'CTO_MAIN'
                THEN  round( ( updated_on - (  select updated_on 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like 'CTO_MAIN:CHECK_FLOWSEND_FORCTO_PROC'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'
                                            ) 
                              )*24  -- hours
                      ,1)                  
            when execution_name = 'SOC_DW_MAIN'
                THEN  round( ( updated_on - created_on
                                        - (  select updated_on - created_on 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                        
                                            )
                                          - (   select updated_on - created_on 
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                                            
                                          ) 
                                          - (   select updated_on - created_on 
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'SOC_FCTS_TRG:CHECK_FAULT_END'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'   )                                                                                                                                               
                              )*24  -- hours
                      ,1)                   
            when execution_name = 'LEVEL0_DAILY' 
                THEN  round( ( (  select updated_on 
                                  from owbsys.all_rt_audit_executions tt 
                                  where
                                      tt.execution_name like 'LEVEL0_DAILY:LEVEL1_FINALIZE'
                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                            AND execution_audit_status = 'COMPLETE'
                                            AND return_result = 'OK'                                  
                               )
                              - created_on
                              - nvl((select updated_on - created_on 
                              from owbsys.all_rt_audit_executions tt 
                              where
                                  tt.execution_name like 'ADSL_SPEED_PRESTAGE:CHECK_NISA_SOURCE'
                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                  AND execution_audit_status = 'COMPLETE'
                                  AND return_result = 'OK'                                  
                                  ),0)                              
                              )*24  -- hours
                      ,1)
            when execution_name = 'PER_MAIN' 
                THEN  round( ( updated_on
                                  - created_on
                                  - (select updated_on - created_on 
                                  from owbsys.all_rt_audit_executions tt 
                                  where
                                      tt.execution_name like 'PER_MAIN:STAGE_CHECK_DWH_ENDSHADOW_PROC'
                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                      AND execution_audit_status = 'COMPLETE'
                                      AND return_result = 'OK'                                  
                                      )
                                  - (select updated_on - created_on 
                                  from owbsys.all_rt_audit_executions tt 
                                  where
                                      tt.execution_name like 'PER_LOADSHADOW_DIM:CHECK_DWH_ENDSHADOW_PROC'
                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                      AND execution_audit_status = 'COMPLETE'
                                      AND return_result = 'OK'                                  
                                       )
                                  - (select updated_on - created_on 
                                  from owbsys.all_rt_audit_executions tt 
                                  where
                                      tt.execution_name like 'PER_MAIN:CHECK_DWH_ENDTARGET_PROC'
                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                      AND execution_audit_status = 'COMPLETE'
                                      AND return_result = 'OK'                                  
                                      )
                                  - (select updated_on - created_on 
                                  from owbsys.all_rt_audit_executions tt 
                                  where
                                      tt.execution_name like 'PER_LOADPRESTAGE_PROM:PRESTAGE_CHECKPROM_PROC'
                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                      AND execution_audit_status = 'COMPLETE'
                                      AND return_result = 'OK'                                  
                                       )                                                                                                      
                              )*24  -- hours
                      ,1)
            when execution_name = 'CMP_RUN_DAILY' 
                THEN  round( ( updated_on - created_on
                                  - (select sum(updated_on - created_on) 
                                  from owbsys.all_rt_audit_executions tt 
                                  where
                                      tt.execution_name like 'CMP_RUN_DAILY:PRERUNCHECK'
                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                      AND execution_audit_status = 'COMPLETE'
                                      AND return_result = 'OK'                                  
                                      )
                                  - (  select max(updated_on - created_on)   
                                          from owbsys.all_rt_audit_executions tt 
                                          where
                                              tt.execution_name like 'FAULT_FCTS_TRG:CHECK_FLOW_WFM_FINNISH'
                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                              AND execution_audit_status = 'COMPLETE'
                                              AND return_result = 'OK'                                                                                                                 
                                 )
                              )*24  -- hours
                      ,1)
            when execution_name = 'OSM_RUN' 
                        THEN  round( ( updated_on - created_on
                                          - (select sum(updated_on - created_on) 
                                          from owbsys.all_rt_audit_executions tt 
                                          where
                                              tt.execution_name like 'OSM_RUN:PRERUNCHECK'
                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                              AND execution_audit_status = 'COMPLETE'
                                              AND return_result = 'OK'                                  
                                              )                                      
                                      )*24  -- hours
                              ,1)
            when execution_name = 'WFM_RUN' 
                        THEN  round( ( updated_on - created_on
                                          - (select sum(updated_on - created_on) 
                                          from owbsys.all_rt_audit_executions tt 
                                          where
                                              tt.execution_name like 'WFM_RUN:PRERUNCHECK'
                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                              AND execution_audit_status = 'COMPLETE'
                                              AND return_result = 'OK'                                  
                                              )                                      
                                      )*24  -- hours
                              ,1)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
            ELSE null
        END duration_real_hrs, -- afairoume tous xronous anamonhs  
       PERCENTILE_CONT(0.8) WITHIN GROUP (
            ORDER BY 
                case when execution_name = 'KPIDW_MAIN'
                                THEN  round( ( updated_on - (  select updated_on 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'KPIDW_MAIN:CHECK_FLOWSEND_FORGLOBAL_PROC'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'
                                                            ) 
                                              )*24  -- hours
                                      ,1)
                            when execution_name = 'CTO_MAIN'
                                THEN  round( ( updated_on - (  select updated_on 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'CTO_MAIN:CHECK_FLOWSEND_FORCTO_PROC'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'
                                                            ) 
                                              )*24  -- hours
                                      ,1)                  
                            when execution_name = 'SOC_DW_MAIN'
                                THEN  round( ( updated_on - created_on
                                                        - (  select updated_on - created_on 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                        
                                                            )
                                                          - (   select updated_on - created_on 
                                                                from owbsys.all_rt_audit_executions tt 
                                                                where
                                                                    tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                    AND execution_audit_status = 'COMPLETE'
                                                                    AND return_result = 'OK'                                                                                                            
                                                          ) 
                                                          - (   select updated_on - created_on 
                                                                from owbsys.all_rt_audit_executions tt 
                                                                where
                                                                    tt.execution_name like 'SOC_FCTS_TRG:CHECK_FAULT_END'
                                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                                    AND execution_audit_status = 'COMPLETE'
                                                                    AND return_result = 'OK'   )                                                                                                                                               
                                              )*24  -- hours
                                      ,1)                   
                            when execution_name = 'LEVEL0_DAILY' 
                                THEN  round( ( (  select updated_on 
                                                  from owbsys.all_rt_audit_executions tt 
                                                  where
                                                      tt.execution_name like 'LEVEL0_DAILY:LEVEL1_FINALIZE'
                                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                  
                                               )
                                              - created_on
                                              - nvl((select updated_on - created_on 
                                              from owbsys.all_rt_audit_executions tt 
                                              where
                                                  tt.execution_name like 'ADSL_SPEED_PRESTAGE:CHECK_NISA_SOURCE'
                                                  AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                  AND execution_audit_status = 'COMPLETE'
                                                  AND return_result = 'OK'                                  
                                                  ),0)                              
                                              )*24  -- hours
                                      ,1)
                            when execution_name = 'PER_MAIN' 
                                THEN  round( ( updated_on
                                                  - created_on
                                                  - (select updated_on - created_on 
                                                  from owbsys.all_rt_audit_executions tt 
                                                  where
                                                      tt.execution_name like 'PER_MAIN:STAGE_CHECK_DWH_ENDSHADOW_PROC'
                                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                      AND execution_audit_status = 'COMPLETE'
                                                      AND return_result = 'OK'                                  
                                                      )
                                                  - (select updated_on - created_on 
                                                  from owbsys.all_rt_audit_executions tt 
                                                  where
                                                      tt.execution_name like 'PER_LOADSHADOW_DIM:CHECK_DWH_ENDSHADOW_PROC'
                                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                      AND execution_audit_status = 'COMPLETE'
                                                      AND return_result = 'OK'                                  
                                                       )
                                                  - (select updated_on - created_on 
                                                  from owbsys.all_rt_audit_executions tt 
                                                  where
                                                      tt.execution_name like 'PER_MAIN:CHECK_DWH_ENDTARGET_PROC'
                                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                      AND execution_audit_status = 'COMPLETE'
                                                      AND return_result = 'OK'                                  
                                                      )
                                                  - (select updated_on - created_on 
                                                  from owbsys.all_rt_audit_executions tt 
                                                  where
                                                      tt.execution_name like 'PER_LOADPRESTAGE_PROM:PRESTAGE_CHECKPROM_PROC'
                                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                      AND execution_audit_status = 'COMPLETE'
                                                      AND return_result = 'OK'                                  
                                                       )                                                                                                      
                                              )*24  -- hours
                                      ,1)
                            when execution_name = 'CMP_RUN_DAILY' 
                                THEN  round( ( updated_on - created_on
                                                  - (select sum(updated_on - created_on) 
                                                  from owbsys.all_rt_audit_executions tt 
                                                  where
                                                      tt.execution_name like 'CMP_RUN_DAILY:PRERUNCHECK'
                                                      AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                      AND execution_audit_status = 'COMPLETE'
                                                      AND return_result = 'OK'                                  
                                                      )
                                                  - (  select max(updated_on - created_on)   
                                                          from owbsys.all_rt_audit_executions tt 
                                                          where
                                                              tt.execution_name like 'FAULT_FCTS_TRG:CHECK_FLOW_WFM_FINNISH'
                                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                              AND execution_audit_status = 'COMPLETE'
                                                              AND return_result = 'OK'                                                                                                                 
                                                 )
                                              )*24  -- hours
                                      ,1)
                            when execution_name = 'OSM_RUN' 
                                        THEN  round( ( updated_on - created_on
                                                          - (select sum(updated_on - created_on) 
                                                          from owbsys.all_rt_audit_executions tt 
                                                          where
                                                              tt.execution_name like 'OSM_RUN:PRERUNCHECK'
                                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                              AND execution_audit_status = 'COMPLETE'
                                                              AND return_result = 'OK'                                  
                                                              )                                      
                                                      )*24  -- hours
                                              ,1)
                            when execution_name = 'WFM_RUN' 
                                        THEN  round( ( updated_on - created_on
                                                          - (select sum(updated_on - created_on) 
                                                          from owbsys.all_rt_audit_executions tt 
                                                          where
                                                              tt.execution_name like 'WFM_RUN:PRERUNCHECK'
                                                              AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                              AND execution_audit_status = 'COMPLETE'
                                                              AND return_result = 'OK'                                  
                                                              )                                      
                                                      )*24  -- hours
                                              ,1)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                            ELSE null
                        END                         
            ASC) 
            OVER (partition by execution_name) duration_real_hrs_p80,                               
       execution_name,
       b.critical_ind
    from owbsys.all_rt_audit_executions a, monitor_dw.dwp_etl_flows b
    where
        a.execution_name = flow_name
        AND flow_name = nvl('&&flow_name',flow_name)
        AND critical_ind = nvl('&&critical_ind',critical_ind)
        and created_on > sysdate - &&days_back
        AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
        AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
        AND execution_audit_status = 'COMPLETE'
        AND return_result = 'OK'
    --order by  execution_name, CREATED_ON desc 
    UNION
    -- the time needed for'SOC_FCTS_TRG:SOC_FLOWPROGR_ENDSOC_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης SOC για NMR
    select root_created_on created_on,
       updated_on updated_on,
       ROUND ( (updated_on - root_created_on) * 24 , 1) duration_hrs_total,
       PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_hrs_total_p80,       
       ROUND (  (updated_on - root_created_on
                 - (  select updated_on - created_on 
                                    from owbsys.all_rt_audit_executions tt 
                                    where
                                        tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = t.ROOT_EXECUTION_AUDIT_ID
                                        AND execution_audit_status = 'COMPLETE'
                                        AND return_result = 'OK'                                        
                                        )
                  - (   select updated_on - created_on 
                        from owbsys.all_rt_audit_executions tt 
                        where
                            tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = t.ROOT_EXECUTION_AUDIT_ID
                            AND execution_audit_status = 'COMPLETE'
                            AND return_result = 'OK'                                                                                                            
                  )        
                ) * 24 , 1)
       duration_real_hrs,
       PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY                                   
                                                ROUND (  (updated_on - root_created_on
                                                 - (  select updated_on - created_on 
                                                                    from owbsys.all_rt_audit_executions tt 
                                                                    where
                                                                        tt.execution_name like 'SOC_DW_MAIN:PRERUNCHECK'
                                                                        AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = t.ROOT_EXECUTION_AUDIT_ID
                                                                        AND execution_audit_status = 'COMPLETE'
                                                                        AND return_result = 'OK'                                        
                                                                        )
                                                  - (   select updated_on - created_on 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'SOC_DW_MAIN:CHECK_LVL0_ENDSHADOW'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = t.ROOT_EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                                                                                            
                                                  )        
                                                ) * 24 , 1) 
                                          ASC) OVER () duration_real_hrs_p80,
       'SOC_for_NMR' execution_name,
       1    CRITICAL_IND
    from (
    with q1 as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
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
                                                AND flow_name = 'SOC_DW_MAIN'--nvl('&&flow_name',flow_name)
                                                AND critical_ind = 1 --nvl('&&critical_ind',critical_ind)
                                                -- restrictions for the main flow
                                                and created_on > sysdate - &&days_back
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                        
                                            )
        -- restricitons for all the nodes (not just the root)    
        AND CREATED_ON > SYSDATE - (&days_back)
        AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
        AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
        AND a.execution_audit_status = 'COMPLETE'
        AND a.return_result = 'OK'
    )                                                                                           
    select /*+ dynamic_sampling (4) */  
       CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
        execution_audit_id,
       SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
       DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
          TYPE,
       created_on,
       updated_on, execution_audit_status,  return_result,
       ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
       ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) ms_until_end_of_root_incl_node,
       CONNECT_BY_ROOT execution_name root_execution_name,
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
       'SOC_for_NMR' = nvl('&&flow_name','SOC_for_NMR')
       --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
       AND execution_name like '%SOC_FLOWPROGR_ENDSOC_ETL' 
       AND CONNECT_BY_ISLEAF = 1 
       AND execution_audit_status = 'COMPLETE'
       AND return_result = 'OK'  
       --------AND CONNECT_BY_ROOT execution_name = b.flow_name
    START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
     --ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
    ) t
    --order by updated_on desc 
    UNION
    -- the time needed for dims to be loaded into shadow
    select root_created_on created_on,
           updated_on updated_on,
           ROUND ( (updated_on - root_created_on) * 24 , 1) duration_hrs_total,
           PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_hrs_total_p80,           
           ROUND ( (updated_on - root_created_on) * 24 , 1) duration_real_hrs,
           PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_real_hrs_p80,
           'LOAD_DIMS_SHADOW' execution_name,
           1 critical_ind
    from (
    with q1 as ( 
        -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
        select /*+ materialize dynamic_sampling (4) */  execution_audit_id, parent_execution_audit_id,
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
                                                AND flow_name = 'LEVEL0_DAILY'--nvl('&&flow_name',flow_name)
                                                AND critical_ind = 1 --nvl('&&critical_ind',critical_ind)
                                                -- restrictions for the main flow
                                                and created_on > sysdate - &&days_back
                                                AND execution_audit_status = 'COMPLETE'
                                                AND return_result = 'OK'                                                        
                                            )
        -- restricitons for all the nodes (not just the root)    
        AND CREATED_ON > SYSDATE - (&&days_back)
        AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
        AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end       
       AND a.execution_audit_status = 'COMPLETE'
       AND a.return_result = 'OK'
    )                                                                                           
    select /*+ dynamic_sampling (4) */  
       CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
        execution_audit_id,
       SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
       DECODE (task_type,
               'PLSQL', 'Mapping',
               'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function')
          TYPE,
       created_on,
       updated_on, execution_audit_status,  return_result,
       ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
       ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 * 60, 1) ms_until_end_of_root_incl_node,
       CONNECT_BY_ROOT execution_name root_execution_name,
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
       'LOAD_DIMS_SHADOW' = nvl('&&flow_name', 'LOAD_DIMS_SHADOW')
       --task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
       AND execution_name like '%PERIF_FLOWPROGR_ENDSHADOW_ETL' 
       AND CONNECT_BY_ISLEAF = 1 
       --AND execution_audit_status = 'COMPLETE'
       --AND return_result = 'OK'  
       --------AND CONNECT_BY_ROOT execution_name = b.flow_name
    START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
    CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
     --ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
    ) t
    --order by end_date desc                     
)
order by  execution_name, CREATED_ON desc
