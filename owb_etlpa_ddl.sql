-- table for storing execution thresholds
  create table monitor_dw.dwp_etl_flows_thresholds (
    flow_name varchar2(100),
    critical_ind  number,
    time_low_bound  number,
    time_high_bound number
  );
  
  insert into monitor_dw.dwp_etl_flows_thresholds (
    flow_name,
    critical_ind)
  select flow_name, critical_ind
  from monitor_dw.dwp_etl_flows;
  
  commit;
  
  alter table monitor_dw.dwp_etl_flows_thresholds rename column time_low_bound to hrs_low_bound;
  alter table monitor_dw.dwp_etl_flows_thresholds rename column time_high_bound to hrs_high_bound;
  
-- View for flows execution times  
  
  /****************
      Query to find the execution of all major flows in the history of interest
      
    Parameters:
    flow_name    (optional) The Main flow name e.g. SOC_DW_MAIN
    critical_ind  (optional)  0/1 if the main flow is critical
    days_back     (optional)  Num of days back from sysdate of required history
    monthly_only      (optional) 1 if we want to filter out all execution and keep the ones created on the 1st day of the month
    Mondays_only      (optional) 1 if we want to filter out all execution and keep the ones created on Mondays    
      
  *****************/
  create or replace view monitor_dw.v_dwp_flows_exec_times
  as 
  select  execution_name,
          monthly_run_ind,
          monday_run_ind,
          created_on,
          updated_on,execution_audit_status, return_result,
          duration_hrs_total,
          dur_hrs_waiting,
          dur_hrs_error,
          dur_hrs_clean,
          PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY dur_hrs_clean ASC) OVER (partition by execution_name) p80_dur_hrs_clean,        
          lag(dur_hrs_clean, 7) over (partition by execution_name order by created_on) prev_7days_hrs_clean,
          (lag(dur_hrs_clean, 7) over (partition by execution_name order by created_on) - dur_hrs_clean) hrs_benefit,         
          p80_duration_hrs_total,
          PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY dur_hrs_waiting ASC) OVER (partition by execution_name) p80_dur_hrs_waiting,
          PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY dur_hrs_error ASC) OVER (partition by execution_name) p80_dur_hrs_error,        
          TOTAL_ERROR_MINS,
          tot_mins_for_resolving_error,
          tot_mins_running_on_error,
          critical_ind,
          execution_audit_id,
          top_level_execution_audit_id
  from (        
      select  t1.execution_name,
              case when to_char(created_on, 'DD') = '01' then 1 else 0 end monthly_run_ind,
              case when trim(to_char(created_on, 'DAY')) = 'MONDAY' then 1 else 0 end monday_run_ind,        
              t1.created_on,
              t1.updated_on, execution_audit_status, return_result,
              t1.duration_hrs_total,
              (t1.duration_hrs_total - t1.duration_real_hrs) dur_hrs_waiting, 
              nvl(round((t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1),0) dur_hrs_error,
              t1.duration_hrs_total - (t1.duration_hrs_total - nvl(t1.duration_real_hrs, t1.duration_hrs_total)) - (nvl(round((t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1),0)) dur_hrs_clean,        
              t1.duration_hrs_total_p80 p80_duration_hrs_total,
            --  t1.duration_real_hrs,
            --  t1.duration_real_hrs_p80,
            --  NVL(ROUND(t1.duration_real_hrs - (t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1), t1.duration_real_hrs) "dur_real_hrs_WITHOUT_errors",
            --  PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY NVL(ROUND(t1.duration_real_hrs - (t2.tot_mins_for_resolving + t2.tot_mins_running_on_error)/60,1), t1.duration_real_hrs) ASC) OVER (partition by execution_name) "dur_real_hrs_WITHOUT_err_p80",
              (t2.tot_mins_for_resolving + t2.tot_mins_running_on_error) TOTAL_ERROR_MINS,        
              t2.tot_mins_for_resolving tot_mins_for_resolving_error, 
              t2.tot_mins_running_on_error,
              t1.critical_ind,
              t1.execution_audit_id,
              t1.top_level_execution_audit_id
      from (
          select execution_audit_id, top_level_execution_audit_id,
            created_on,
             updated_on, execution_audit_status, return_result,
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
                  when execution_name = 'PENDORD_RUN' 
                              THEN  round( ( updated_on - created_on
                                                - (select sum(updated_on - created_on) 
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'PENDORD_RUN:PRERUNCHECK'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID 
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                  
                                                    )
                                                - (select sum(updated_on - created_on) 
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'PENDORD_RUN:BUS_PRERUN_CHECK'
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
              and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
              --and created_on > sysdate - &&days_back
              AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
              AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
             -- AND execution_audit_status = 'COMPLETE'
             -- AND return_result = 'OK'
          --order by  execution_name, CREATED_ON desc 
          UNION
          -- the time needed for'SOC_FCTS_TRG:SOC_FLOWPROGR_ENDSOC_ETL' -- ενημέρωση ημερομηνίας ολοκλήρωσης SOC για NMR
          select execution_audit_id, top_level_execution_audit_id, root_created_on created_on,
             updated_on updated_on,
             execution_audit_status, return_result,
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
                  select /*+ materialize dynamic_sampling (4) */  execution_audit_id, top_level_execution_audit_id, parent_execution_audit_id,
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
                                                          and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                                                          --and created_on > sysdate - &&days_back
                                                          --AND execution_audit_status = 'COMPLETE'
                                                          --AND return_result = 'OK'                                                        
                                                      )
                  -- restricitons for all the nodes (not just the root)    
                  --AND CREATED_ON > SYSDATE - (&days_back)
                  and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                  AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
                  AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
                 -- AND a.execution_audit_status = 'COMPLETE'
                  --AND a.return_result = 'OK'
              )                                                                                           
              select /*+ dynamic_sampling (4) */  
                 CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
                  execution_audit_id, top_level_execution_audit_id,
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
          select execution_audit_id, top_level_execution_audit_id, root_created_on created_on,
                 updated_on updated_on, execution_audit_status, return_result,
                 ROUND ( (updated_on - root_created_on) * 24 , 1) duration_hrs_total,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_hrs_total_p80,           
                 ROUND ( (updated_on - root_created_on) * 24 , 1) duration_real_hrs,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - root_created_on) * 24 , 1) ASC) OVER () duration_real_hrs_p80,
                 'LOAD_DIMS_SHADOW' execution_name,
                 1 critical_ind
          from (
          with q1 as ( 
              -- Subquery factoring to filter only the execution rows of interest and then run the connect by query which is heavy
              select /*+ materialize dynamic_sampling (4) */  execution_audit_id, top_level_execution_audit_id, parent_execution_audit_id,
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
                                                      and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
                                                      --and created_on > sysdate - &&days_back
                                                     -- AND execution_audit_status = 'COMPLETE'
                                                     -- AND return_result = 'OK'                                                        
                                                  )
              -- restricitons for all the nodes (not just the root)    
              --AND CREATED_ON > SYSDATE - (&&days_back)
              and created_on between  nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) - &&days_back AND nvl(to_date('&&from_date','dd/mm/yyyy') ,sysdate) + 1
              AND to_char(created_on, 'DD') = case when nvl('&&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
              AND trim(to_char(created_on, 'DAY')) = case when nvl('&&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end       
            -- AND a.execution_audit_status = 'COMPLETE'
            -- AND a.return_result = 'OK'
          )                                                                                           
          select /*+ dynamic_sampling (4) */  
             CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
              execution_audit_id, top_level_execution_audit_id,
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
             AND execution_audit_status = 'COMPLETE'
             AND return_result = 'OK'  
             --------AND CONNECT_BY_ROOT execution_name = b.flow_name
          START WITH a.PARENT_EXECUTION_AUDIT_ID IS NULL
          CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
           --ORDER BY root_execution_name, root_execution_audit_id DESC, execution_audit_id desc--, duration_mins DESC;
          ) t
          --order by end_date desc                     
      ) t1,
      (      
      --**** Calculate the time spent on error for a major Flow
      select  nvl(sum(mins_for_resolving),0) tot_mins_for_resolving,
              nvl(sum(mins_running_on_error),0) tot_mins_running_on_error, 
              root_execution_name, root_created_on, root_updated_on, root_execution_audit_id         
      from
      (   -- detailed query showing the individual mappings on error
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
               AND to_char(created_on, 'DD') = case when nvl('&montlh_only',0) = 1 then '01' else to_char(created_on, 'DD') end    
               AND trim(to_char(created_on, 'DAY')) = case when nvl('&mondays_only',0) = 1 then 'MONDAY' else trim(to_char(created_on, 'DAY')) end
             --AND a.execution_audit_status = 'COMPLETE'
             --AND a.return_result = 'OK'
          )
          select  case when tt.result = 'OK' THEN ROUND((TT.CREATED_ON - tt.PREV_UPDATED_ON) * 24 * 60, 1) else null end mins_for_resolving, --ROUND((TT.CREATED_ON - tt.PREV_UPDATED_ON) * 24 * 60, 1) mins_for_resolving,
                  case when tt.result in ('FAILURE', 'OK_WITH_WARNINGS') THEN  ROUND((TT.UPDATED_ON - tt.CREATED_ON) * 24 * 60, 1) else null end mins_running_on_error,
                  tt.*
          from (                                                                                        
              select /*+ dynamic_sampling (4) */  
                 -- label rows where result = 'failure' or the 1st OK result after a failure
                 case when (q1.return_result in ('OK') AND lag(return_result,1) over (partition by CONNECT_BY_ROOT execution_audit_id, execution_name order by q1.updated_on ) in ('FAILURE', 'OK_WITH_WARNINGS')) THEN 1
                      when  q1.return_result in ('FAILURE', 'OK_WITH_WARNINGS') THEN 1
                      ELSE 0
                 end ind,           
                 CONNECT_BY_ROOT execution_audit_id root_execution_audit_id,
                 execution_audit_id,
                 CONNECT_BY_ROOT execution_name root_execution_name,   
                 execution_name,
                 SUBSTR (execution_name, INSTR (execution_name, ':') + 1) execution_name_short,
                 DECODE (task_type,
                         'PLSQL', 'Mapping',
                         'PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type)    TYPE,
                 b.CREATION_DATE error_creation_date,
                  q1.RETURN_RESULT RESULT,
                  b.SEVERITY,
                  c.PLAIN_TEXT,        
                 created_on,
                 updated_on,
                 lag(updated_on,1) over (partition by CONNECT_BY_ROOT execution_audit_id, execution_name order by q1.updated_on) prev_updated_on, 
                 execution_audit_status,  return_result,
                 ROUND ( (updated_on - created_on) * 24 * 60, 1) duration_mins,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - created_on) * 24 * 60, 1) ASC) OVER (partition by execution_name) duration_mins_p80,   
                 ROUND((
                          (   select c.updated_on 
                              from owbsys.all_rt_audit_executions c 
                              where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                                    AND  c.execution_name like trim('%'||nvl('&&mstone_node_name','xxxxxx'))
                          ) 
                      - created_on) * 24 , 1) hr_unt_end_of_mstone_incl_node,
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((
                          (   select c.updated_on 
                              from owbsys.all_rt_audit_executions c 
                              where c.TOP_LEVEL_EXECUTION_AUDIT_ID = q1.top_level_execution_audit_id 
                                    AND  c.execution_name like trim('%'||nvl('&mstone_node_name','xxxxxx'))
                          ) 
                      - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_unt_end_of_mstone_p80,        
                 ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1) hr_until_end_of_root_incl_node,   
                 PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND((CONNECT_BY_ROOT updated_on - created_on) * 24 , 1)  ASC) OVER (partition by execution_name) hrs_until_end_of_root_p80,      
                 --b.critical_ind,
                 ROUND ( (CONNECT_BY_ROOT updated_on - CONNECT_BY_ROOT created_on) * 24, 1) root_duration_hrs,
                 CONNECT_BY_ROOT created_on root_created_on,
                 CONNECT_BY_ROOT updated_on root_updated_on,
                 CONNECT_BY_ISLEAF "IsLeaf",
                 LEVEL,
                 SYS_CONNECT_BY_PATH (
                    SUBSTR (execution_name, INSTR (execution_name, ':') + 1),      '/') path  
              from q1,owbsys.WB_RT_AUDIT_MESSAGES b, owbsys.WB_RT_AUDIT_MESSAGE_LINES c
              WHERE
                 q1.EXECUTION_AUDIT_ID= b.AUDIT_EXECUTION_ID(+)  
                 AND  b.AUDIT_MESSAGE_ID = c.AUDIT_MESSAGE_ID(+)
                 AND q1.return_result in ('FAILURE', 'OK','OK_WITH_WARNINGS') 
                 AND task_type IN ('PLSQL', 'PLSQLProcedure', 'PLSQLFunction')
                 --execution_name like trim('%&node_name')
                 AND CONNECT_BY_ISLEAF = 1 
                 --AND execution_audit_status = 'COMPLETE'
                 --AND return_result = 'OK'  
                 --------AND CONNECT_BY_ROOT execution_name = b.flow_name
              START WITH PARENT_EXECUTION_AUDIT_ID IS NULL
              CONNECT BY PRIOR execution_audit_id = parent_execution_audit_id
          ) tt
          where
              tt.ind = 1
          --ORDER BY root_execution_name, root_execution_audit_id DESC, path, error_creation_date desc--, duration_mins DESC;
      ) ttt
      where
          mins_for_resolving > 0 or mins_running_on_error > 0
      group by root_execution_name, root_created_on, root_updated_on, root_execution_audit_id
      --order by  root_execution_name, root_created_on desc    
      ) t2
      where
          t1.execution_audit_id = t2.root_execution_audit_id(+)     
    --  order by  execution_name, CREATED_ON desc
  ) final
  order by  execution_name, CREATED_ON desc;
  
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

grant execute on nkarag.eval to monitor_dw;

create or replace view monitor_dw.v_dwp_flows_critical_path
as
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
                 nkarag.EVAL(SYS_CONNECT_BY_PATH(NVL(TO_CHAR(duration_mins),'NULL'),'+')) PATH_DURATION_MINS,               
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
                                  AND a.created_on between prev.updated_on and (prev.updated_on + 1/24/60/60 )-- sequential execution
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
  order by root_created_on desc, path_duration_mins desc, seq_length desc;    

  