DROP VIEW MONITOR_DW.V_DWP_FLOWS_EXEC_TIMES;

-- critical_ind = 1
-- 15 days back 
CREATE OR REPLACE FORCE VIEW MONITOR_DW.V_DWP_FLOWS_EXEC_TIMES
(
   EXECUTION_NAME,
   MONTHLY_RUN_IND,
   MONDAY_RUN_IND,
   CREATED_ON,
   UPDATED_ON,
   EXECUTION_AUDIT_STATUS,
   RETURN_RESULT,
   DURATION_HRS_TOTAL,
   DUR_HRS_WAITING,
   DUR_HRS_ERROR,
   DUR_HRS_CLEAN,
   P80_DUR_HRS_CLEAN,
   PREV_7DAYS_HRS_CLEAN,
   HRS_BENEFIT,
   P80_DURATION_HRS_TOTAL,
   P80_DUR_HRS_WAITING,
   P80_DUR_HRS_ERROR,
   TOTAL_ERROR_MINS,
   TOT_MINS_FOR_RESOLVING_ERROR,
   TOT_MINS_RUNNING_ON_ERROR,
   CRITICAL_IND,
   EXECUTION_AUDIT_ID,
   TOP_LEVEL_EXECUTION_AUDIT_ID
)
AS
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
                                        - ( -- this returns no rows for non-monthly flows. Use sum to fix it
                                                select nvl(sum(updated_on - created_on),0)
                                                from owbsys.all_rt_audit_executions tt 
                                                where
                                                    tt.execution_name like 'PER_LOADKPIS_MON_SNP:CHECK_DWH_ENDKPIDWGLOBAL_PROC'
                                                    AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                    AND execution_audit_status = 'COMPLETE'
                                                    AND return_result = 'OK'                                                                                      
                                        )
                                        - (select updated_on - created_on 
                                        from owbsys.all_rt_audit_executions tt 
                                        where
                                            tt.execution_name like 'PER_LOADPRESENT_OPERREP:PRESENT_CHECK_ORDPEND_END_PROC'
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
                  when execution_name = 'PATCH_UNBOUNDED_OL'
                      THEN  round( ( updated_on - (  select updated_on 
                                              from owbsys.all_rt_audit_executions tt 
                                              where
                                                  tt.execution_name like 'PATCH_UNBOUNDED_OL:INITIALIZE_PROC'
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
                                                        - ( -- this returns no rows for non-monthly flows. Use sum to fix it
                                                            select nvl(sum(updated_on - created_on),0)
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'PER_LOADKPIS_MON_SNP:CHECK_DWH_ENDKPIDWGLOBAL_PROC'
                                                            AND  TT.TOP_LEVEL_EXECUTION_AUDIT_ID = A.EXECUTION_AUDIT_ID
                                                            AND execution_audit_status = 'COMPLETE'
                                                            AND return_result = 'OK'                                  
                                                             )    
                                                        - (select updated_on - created_on 
                                                        from owbsys.all_rt_audit_executions tt 
                                                        where
                                                            tt.execution_name like 'PER_LOADPRESENT_OPERREP:PRESENT_CHECK_ORDPEND_END_PROC'
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
                                  when execution_name = 'PATCH_UNBOUNDED_OL'
                                      THEN  round( ( updated_on - (  select updated_on 
                                                              from owbsys.all_rt_audit_executions tt 
                                                              where
                                                                  tt.execution_name like 'PATCH_UNBOUNDED_OL:INITIALIZE_PROC'
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