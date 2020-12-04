------------------------------------------------------
--  fsess_owb_proc
--
--  Run script fsess_owb to select rows with a non-NULL value in "Problem / Action"
--  and insert results into a table (monitor_dw.fsess_owb).
--
------------------------------------------------------
create or replace procedure monitor_dw.fsess_owb_proc
IS
    l_monthly_flow_ind  number;   
    l_dw_last_run_updated   number;
    l_owb_node_name     varchar2(100); 
    l_days_back         pls_integer;
    l_prfrmnce_thrshld  pls_integer;
    l_mins_on_wait_thrshld  pls_integer;
    l_message           CLOB; --varchar2(4000);
    l_msg_tmp           varchar2(4000);
    l_rows_returned     number;
    l_cnt               number;
    l_target_dt         date;
    l_task_no_next      number;
BEGIN

    -- check if OTE_DW_LAST_RUN has been updated
    -- nvl and sum for the case where no rows are returned
    select nvl(sum(case when execution_audit_status = 'COMPLETE' then 1 -- OTE_DW_LAST_RUN has been updated
                else 0 -- OTE_DW_LAST_RUN has not been updated yet
            end),0) dw_last_run_updated   into l_dw_last_run_updated       
    from owbsys.all_rt_audit_executions 
    where 1=1 
        AND execution_name  = 'LEVEL1_FINALIZE:DW_UPDATE_LAST_RUN_DATE'
        AND trunc(created_on) = trunc(sysdate);

    -- check if monthly flows are running
    select      CASE    WHEN   (select run_date from stage_dw.dw_control_table where procedure_name = 'LAST_RUN_END_TIME') < sysdate  -- LEVEL0 is not running 
                            AND last_day((select run_date from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')) = (select run_date /*date'2015-08-31'*/ from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN') 
                          THEN    1
                        WHEN    (select run_date from stage_dw.dw_control_table where procedure_name = 'LAST_RUN_END_TIME') > sysdate -- LEVEL0 is running
                                AND
                                last_day((select run_date + decode(l_dw_last_run_updated, 0, 1, 0) from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')) = (select run_date + decode(l_dw_last_run_updated, 0, 1, 0) /*date'2015-08-31'*/ from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')
                          THEN 1
                        ELSE 0
                END monthly_flow_ind into l_monthly_flow_ind
    from dual;     
    
    l_target_dt := sysdate;
    insert into monitor_dw.fsess_owb
    with owb_execs
    as ( -- owb executions of interest
      -- rewrite the query directly over OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS instead of the view owbsys.all_rt_audit_executions
        --  for better performance 10 secs instead of 30!
     select  /*+ qb_name(owb_execs) materialize no_merge  */ audit_execution_id owb_audit_id,
            execution_name,
            SUBSTR (execution_name, INSTR (execution_name, ':') + 1) owb_name,
            (select execution_name from OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS where audit_execution_id = t1.top_level_audit_execution_id) owb_flow,
            DECODE (t2.operator_name,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', t2.operator_name) owb_type,
            OWBSYS.wb_rt_constants.to_string (audit_status) owb_status,
            return_result owb_result,
            creation_date owb_created_on,
            last_update_date owb_updated_on,
            ROUND ( (case when last_update_date = creation_date then sysdate else last_update_date end - creation_date) * 24 * 60, 1) duration_mins,
            PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (last_update_date - creation_date) * 24 * 60, 1) ASC) OVER (partition by execution_name) 
                duration_mins_p80            
    from OWBSYS.OWB$WB_RT_AUDIT_EXECUTIONS t1, OWBSYS.wb_rt_def_execution_operators t2
    where 1=1
        --AND audit_status = 'BUSY' -- only running nodes
        AND SUBSTR (execution_name, INSTR (execution_name, ':') + 1) = nvl(upper(trim(l_owb_node_name)), SUBSTR (execution_name, INSTR (execution_name, ':') + 1))
        AND creation_date > sysdate - nvl(l_days_back, case when l_monthly_flow_ind = 0 then 15 else 100 end)  -- for checking execution time in history
        AND to_char(creation_date, 'DD') = case when l_monthly_flow_ind = 1 then '01' else to_char(creation_date, 'DD') end 
        --AND creation_date > sysdate - 1 -- for performance reasons!
        AND nvl(return_result,'OK') = 'OK' -- take only running nodes, or succesfullly complete
        AND t1.execution_operator_id = t2.execution_operator_id(+)
    ),
    owb_sessions
    as( --  these are the DB sessions directly joined to all_rt_audit_executions 
        --  (note: these are not all relevant sessions, since there are also the generated ones from procedures)
        select /*+ qb_name(owb_sessions) no_merge  */ t1.*, t2.owb_name --, t2.*
        from gv$session t1 join owb_execs t2 on (t1.client_info = t2.owb_audit_id)
        where 1=1
    --          AND client_info in (select owb_audit_id from owb_execs)
            AND client_info IS NOT NULL 
            AND regexp_like (client_info,'^\d+$') -- only numeric client_info value, so that you avoid an ORA-01722:invalid number
        order by t2.owb_flow    
    ),
    owb_sid_all
    as ( -- get all relevant sessions, even the generated ones from a PL/SQL  procedure call
        select /*+ qb_name(owb_sid_all) materialize no_merge */ *
        from (
            select /*+ qb_name(owb_sid_all_1) */ inst_id, sid, serial#  from owb_sessions
            union
            select /*+ qb_name(owb_sid_all_2) */ inst_id, sid, serial# from gv$session -- get generated sessions (e.g. sessions invoked by a call to DBMS_MVIEW.refresh)
            where PLSQL_ENTRY_OBJECT_ID in (select plsql_entry_object_id from owb_sessions)
            union   --  also in the case of an MV refresh OWB node, 
                    --  get also all sessions of the same user that have a PLSQL_ENTRY_OBJECT_ID = DBMS_SNAPSHOT.REFRESH
                    --      and have started (SQL_EXEC_START) after the parent session (i.e., the owb procedure that triggers the refresh)
            select /*+ qb_name(owb_sid_all_3) */ inst_id, sid, serial# 
            from gv$session
            where
                1 = case when (select count(*) from owb_sessions where owb_name like '%REFRESH%') > 0 THEN 1 ELSE 0 END
                AND    PLSQL_ENTRY_OBJECT_ID in (select object_id from dba_procedures where object_name = 'DBMS_SNAPSHOT' and procedure_name like 'REFRESH%')
                AND    username in (select username from owb_sessions where owb_name like '%REFRESH%')
                AND SQL_EXEC_START > (select min(SQL_EXEC_START) from owb_sessions where owb_name like '%REFRESH%')
        )            
    ),
    owb_sessions_all
    as(
        select /*+ qb_name(owb_sessions_all) materialize no_merge  */ *
        from gv$session
        where (inst_id, sid, serial#) in (select inst_id, sid, serial# from owb_sid_all)
    ),
    corrupted_nodes
    as( -- find OWB nodes which are "BUSY" but have no DB sessions (i.e. corrupted)
        select *
        from owb_execs
        where
            owb_audit_id not in (select nvl(client_info, -1) from owb_sessions_all)
            and owb_status = 'BUSY'
            and owb_type <> 'ProcessFlow'
            and duration_mins > 120
            and trunc(owb_created_on) = trunc(sysdate) -- only for today - dont repaat rt_audit_executions "garbage" of the last 15 days
    ),
    final
    as (
        select /*+   qb_name(final)
                    leading(t1 t2 t3 t4 t5) 
                    use_hash(t2 t3 t4 t5) 
                    no_swap_join_inputs(t2)
                    no_swap_join_inputs(t3)
                    no_swap_join_inputs(t4)
                    no_swap_join_inputs(t5)
               */
             t2.*, t1.*, t3.owner obj_owner, t3.object_name obj_name, t3.object_type obj_type, T4.PLAN_HASH_VALUE, t4.executions, t4.sql_text,T5.PLAN_HASH_VALUE prev_plan_hash_value, t5.executions prev_executions, t5.sql_text prev_sql_text
        from owb_sessions_all t1 
                left outer join owb_execs t2 on (t1.client_info = t2.owb_audit_id)
                    left outer join dba_objects t3 on  (t1.ROW_WAIT_OBJ# = t3.object_id) 
                        left outer join gv$sql t4 on (t1.sql_id = T4.SQL_ID AND t1.sql_child_number = T4.CHILD_NUMBER AND t1.inst_id = t4.inst_id)
                            left outer join gv$sql t5 on (t1.prev_sql_id = T5.SQL_ID AND t1.prev_child_number = T5.CHILD_NUMBER AND t1.inst_id = t5.inst_id)
    ),
    ff
    as(  
        select  /*+  qb_name(main)
                     PUSH_SUBQ(@monthly_flow)
                     PUSH_SUBQ(@owb_execs) 
                     PUSH_SUBQ(@owb_sessions)
                 */                 
                --*** Diagnosis
            -- Low Performance
                -- light tasks
        case    when    owb_type <> 'ProcessFlow' AND owb_name NOT LIKE '%CHECK%'  AND nullif(duration_mins_p80,0) < 5 AND duration_mins > 30
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer') -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- medium tasks                                
                when    owb_type <> 'ProcessFlow' AND owb_name NOT LIKE '%CHECK%' AND nullif(duration_mins_p80,0) between 5 AND 15 AND duration_mins > 50 
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer') -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- heavy tasks                      
                when    owb_type <> 'ProcessFlow' AND owb_name NOT LIKE '%CHECK%' AND nullif(duration_mins_p80,0) > 15 AND duration_mins > 60 
                        AND round(duration_mins/nullif(duration_mins_p80,0),1) >= nvl(l_prfrmnce_thrshld, 3)
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer')  -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- non OWB tasks                    
                when    nvl(owb_type, 'xx') <> 'ProcessFlow' AND ((duration_mins_p80 IS NULL) or (duration_mins_p80 = 0)) 
                        AND nvl(duration_mins, round((sysdate - sql_exec_start)* 24 * 60,1)) > 120 
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer')  -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- tasks sleeping                    
                when    owb_type <> 'ProcessFlow'  AND duration_mins > 120
                        AND round(duration_mins/nullif(duration_mins_p80,0),1) >= nvl(l_prfrmnce_thrshld, 3)
                        AND (state = 'WAITING' AND event = 'PL/SQL lock timer')   
                    THEN  '"DW TASK IS SLEEPING (event ''PL/SQL lock timer'') FOR TOO LONG"'                                                                                      
        /*        when   round(duration_mins/nullif(duration_mins_p80,0),1) >= nvl(l_prfrmnce_thrshld, 2) 
                            AND duration_mins > 60 then '"LOW PERFORMANCE"'*/
                     else null
                end
                ||
                    -- Waiting on an "non-working" wait event
                case when   state = 'WAITING' 
                          --  AND wait_class not in ('User I/O', 'Idle', 'Network')
                            AND ((wait_class not in ('User I/O', 'Idle', 'Network','Application', 'Concurrency'))
                                    OR
                                 (wait_class in ('Application', 'Concurrency') AND blocking_session is null) 
                                )                            
                            AND round(WAIT_TIME_MICRO/1e6) > 60* nvl(l_mins_on_wait_thrshld, 30) then '- "PROBLEMATIC WAIT FOR TOO LONG" / "CALL DBA"'  
                     else null
                end         
                ||
                    -- Session blocked by another session
                case when   blocking_session is not null
                                -- not blocked by another parallel slave of the same query 
                            AND sql_id <> (select sql_id from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session)
                                -- blocked for more than  mins_on_wait_thrshld
                            AND (
                                    -- get waited time for v$session_event and not for v$session, in order to catch cases where a blocked call is looping and thus the waited time is instanteneous but over all is significant.
                                    (   select (TIME_WAITED/100) TIME_WAITED_SECS 
                                        from gv$session_event 
                                        where inst_id = f.inst_id and  sid = f.sid and event = f.event
                                              and wait_class in ('Application', 'Concurrency') 
                                    ) > 60* nvl(l_mins_on_wait_thrshld, 30)
                                    OR
                                    WAIT_TIME_MICRO/1e6 > 60* nvl(l_mins_on_wait_thrshld, 30)
                                ) 
                            then '- "SESSION BLOCKED" / "KILL BLOCKER IF APPROPRIATE"'
                     else null
                end
                    "Problem / Action",
                --*** OWB stuff
                owb_flow,
                username,
                owb_name,
                owb_type,
                owb_audit_id,       
                owb_created_on,
                owb_updated_on,
                owb_status,
                owb_result,
                nvl(duration_mins, round((sysdate - logon_time)* 24 * 60,1)) duration_mins,
                duration_mins_p80    owb_duration_mins_p80,
                round(duration_mins/nullif(duration_mins_p80,0),1)  owb_times_exceeding_p80,
                --*** session identifiers 
                inst_id,
                sid,
                serial#,
                logon_time,
                status,
                program prog,
                --*** running sql
                sql_id,
                sql_child_number,
                sql_exec_start,
                plan_hash_value,
                sql_text,
                (select owner||'.'||object_name||'.'||procedure_name from dba_procedures where object_id = f.plsql_entry_object_id and subprogram_id = f.PLSQL_ENTRY_SUBPROGRAM_ID)
                    entry_plsql_proc,
                --*** wait info
                CASE WHEN state != 'WAITING' THEN 'WORKING'
                     ELSE 'WAITING'
                END 
                    wait_state,  
                CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
                     ELSE WAIT_CLASS
                END                          
                   WAIT_CLASS,
                CASE WHEN state != 'WAITING' THEN 'On CPU / runqueue'
                ELSE event
                END 
                    event,
                CASE WHEN state != 'WAITING'  THEN  '(last wait) '  
                else ''
                END || round(WAIT_TIME_MICRO/1e6,1)
                    secs_in_wait,
        --            WAIT_TIME_MICRO/1e6,
        --            TIME_SINCE_LAST_WAIT_MICRO/1e6,
                --*** object waiting for
                obj_owner,
                obj_name,
                obj_type,            
                --*** blocking session info
                blocking_instance,
                blocking_session,
                (select username from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session) 
                    blocker,
                (select SUBSTR (execution_name, INSTR (execution_name, ':') + 1) from owbsys.all_rt_audit_executions where execution_audit_id = (select client_info from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session and regexp_like (client_info,'^\d+$')))
                    blocker_owb_node,
                (select execution_name from owbsys.all_rt_audit_executions where execution_audit_id = (select top_level_execution_audit_id from owbsys.all_rt_audit_executions  where execution_audit_id = (select client_info from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session and regexp_like (client_info,'^\d+$')))) 
                    blocker_main_flow,                        
                (select 'exec kill_session('||sid||', '||serial#||', '||inst_id||')' from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session)
                    kill_blocker_stmnt,                                                         
                --*** OS identifiers
                osuser,
                process osprocess,
                machine,
                port,
                terminal,                    
                --*** previous sql
                prev_sql_id,
                prev_child_number,
                prev_exec_start,
                prev_plan_hash_value,
                prev_sql_text        
        from final f  
        union all
        select  '"OWB task in "BUSY" state, with no corresponding DB session (possible corruption) / Retry node from OWF Monitor"' "Problem / Action",
            --*** OWB stuff
            owb_flow,
            null username,
            owb_name,
            owb_type,
            owb_audit_id,       
            owb_created_on,
            owb_updated_on,
            owb_status,
            owb_result,
            duration_mins,
            duration_mins_p80    owb_duration_mins_p80,
            round(duration_mins/nullif(duration_mins_p80,0),1)  owb_times_exceeding_p80,
            --*** session identifiers 
            null inst_id,
            null sid,
            null serial#,
            null logon_time,
            null status,
            null  prog,
            --*** running sql
            null sql_id,
            null sql_child_number,
            null sql_exec_start,
            null plan_hash_value,
            null sql_text,
            null entry_plsql_proc,
            --*** wait info
            null wait_state,  
            null WAIT_CLASS,
            null event,
            null secs_in_wait,
            null obj_owner,
            null obj_name,
            null obj_type,            
            --*** blocking session info
            null blocking_instance,
            null blocking_session,
            null blocker,
            null blocker_owb_node,
            null blocker_main_flow,                        
            null kill_blocker_stmnt,                                                         
            --*** OS identifiers
            null osuser,
            null  osprocess,
            null machine,
            null port,
            null terminal,                    
            --*** previous sql
            null prev_sql_id,
            null prev_child_number,
            null prev_exec_start,
            null prev_plan_hash_value,
            null prev_sql_text               
        from corrupted_nodes      
        order by owb_flow, username, owb_name
    )
    select  l_target_dt --sysdate
            , t.*
    from ff t
    where 1=1
         AND "Problem / Action" IS NOT NULL;
         
    commit;
    
    -- send mail - only for the rows just inserted and only if you have not send this mail again
    l_rows_returned := 0;
    l_cnt := 0;
  --  DBMS_LOB.CREATETEMPORARY(l_message,TRUE, dbms_lob.session);
    l_message := '';
    l_msg_tmp := 'Snapshot Time: '||to_char(l_target_dt,'dd/mm/yyyy hh24:mi:ss')||chr(10)||chr(10)||
                  '________________________________________________________________________________'||chr(10)||
                 'NOTE: You can view the same info by running the following query:'||chr(10) ||chr(10) ||
                 'select * '||chr(10)||                 
                 'from monitor_dw.fsess_owb '||chr(10)||
                 'where snapshot_dt = to_date('''||to_char(l_target_dt,'dd/mm/yyyy hh24:mi:ss')||''', ''dd/mm/yyyy hh24:mi:ss'')'||chr(10)||
                 'order by username, nvl(owb_flow, sql_id), owb_name, prog;'||chr(10)||
                 '________________________________________________________________________________'||chr(10)||chr(10);
    
    l_message := l_message || l_msg_tmp;
    
    -- Master info loop
    for r in (
        select  snapshot_dt,
                "Problem / Action",
                case when "Problem / Action" in ('"LOW PERFORMANCE"','"OWB task in "BUSY" state, with no corresponding DB session (possible corruption) / Retry node from OWF Monitor"' ) then 'Dev STANDBY BI' ELSE 'AS Mediation Support' end WHO2CALL,
                owb_flow,
                username,
                owb_name,
                owb_type,
                owb_audit_id,
                owb_created_on,
                owb_updated_on,
                owb_status,
                owb_result,
                duration_mins,
                owb_duration_mins_p80,
                owb_times_exceeding_p80,
                sql_id,
                sql_child_number,
                plan_hash_value,
            --    entry_plsql_proc,
                sql_exec_start,
                sql_text      
          from (
                    select t.*
                    from monitor_dw.fsess_owb t
                    where   snapshot_dt = l_target_dt -- snapshot_dt > sysdate -  2/60/24 --trunc(sysdate, 'MI') = trunc(snapshot_dt, 'MI'
                            -- dont send the same mail again, if it has been sent within the same day / in the last 3 hours!                           
                            and ("Problem / Action", nvl(to_char(OWB_AUDIT_ID), SQL_ID)) not in ( --and ("Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id)) not in ( -- dont send the same mail again, if it has been sent in the last 3 hours!
                                select nvl("Problem / Action", 'lala'), nvl(to_char(OWB_AUDIT_ID), SQL_ID) --nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id) 
                                from monitor_dw.fsess_owb
                                where 
                                    snapshot_dt < t.snapshot_dt
                                    AND snapshot_dt > sysdate - 180/60/24 --trunc(snapshot_dt) = trunc(t.snapshot_dt) 
                            )
                    order by username, nvl(owb_flow, sql_id), owb_name, prog
          )
        group by snapshot_dt,
                "Problem / Action",
                owb_flow,
                username,
                owb_name,
                owb_type,
                owb_audit_id,
                owb_created_on,
                owb_updated_on,
                owb_status,
                owb_result,
                duration_mins,
                owb_duration_mins_p80,
                owb_times_exceeding_p80,
                sql_id,
                sql_child_number,
                plan_hash_value,
          --      entry_plsql_proc,                
                sql_exec_start,
                sql_text                           
        order by owb_flow, owb_name, username                          
    )
    loop
        l_rows_returned := 1;
        l_cnt := l_cnt + 1;
            -- build message body
        l_msg_tmp := 
                    '==============================================================='||chr(10)||
                    '|                  Problem in DW Running Task (#'||l_cnt||')'||chr(10)||
                    '|                -----------------------------------------'||chr(10)||
                    '|'||chr(10)||
                    '|   ***Problem / Action***:    '||r."Problem / Action"||chr(10)||
                    '|'||chr(10)||
                    '|   (*Call*: '||r.WHO2CALL||')'||chr(10)||
                    '|   -----------'||chr(10)||                    
                    '|   OWB Task details'||chr(10)||
                    '|   -----------------'||chr(10)||
                    '|   Main Flow:              '||r.owb_flow||chr(10)||
                    '|   DB Username:               '||r.username||chr(10)||
                    '|   Node Name:              '||r.owb_name||chr(10)||
                    '|   Node Type:              '||r.owb_type||chr(10)||
                    '|   Task Created on:             '||to_char(r.owb_created_on,'dd/mm/yyyy hh24:mi:ss')||chr(10)||
                    '|   Status:                 '||r.owb_status||chr(10)||                   
                    '|   Duration (mins) p80:    '||r.owb_duration_mins_p80||chr(10)||
                    '|   ***Duration (mins)***:  '||r.duration_mins||chr(10)||
                    '|   Times exceeding p80:    '||r.owb_times_exceeding_p80||chr(10)||
                    '|'||chr(10)||
                    '|   -----------'||chr(10)||                    
                    '|   SQL details'||chr(10)||
                    '|   -----------'||chr(10)||
                    '|   sql_id:            '||r.sql_id||chr(10)||
                    '|   sql_child_number:  '||r.sql_child_number||chr(10)||
                    '|   plan_hash_value:   '||r.plan_hash_value||chr(10)||                    
                    '|   sql_exec_start:    '||to_char(r.sql_exec_start,'dd/mm/yyyy hh24:mi:ss')||chr(10)||
                    '|   sql_text:          '||substr(r.sql_text,1,120)||chr(10)||   
                --    '|   Entry PL/SQL Proc:     '||r.entry_plsql_proc||chr(10)||
                    '|'||chr(10)||
                    '==============================================================='||chr(10);    
        --dbms_lob.writeappend(lob_loc => l_message, amount => length(l_msg_tmp), buffer  => l_msg_tmp);  
        l_message := l_message || l_msg_tmp;                                               
    end loop;
    
   l_msg_tmp := chr(10)||chr(10)||
                chr(9)||chr(9)||chr(9)||chr(9)||'*-*-*-*-*-*-*-*-*-**-*-*-*-*-*-*-*-*-**-*-*-*-*-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*'||chr(10)||
                chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||chr(9)||'                  Detailed Info Section for Troubleshooting                  '||chr(10)||
                chr(9)||chr(9)||chr(9)||chr(9)||'*-*-*-*-*-*-*-*-*-**-*-*-*-*-*-*-*-*-**-*-*-*-*-*-*-*-*-**-*-*-*-*-*-*-*-*-*-*'||chr(10)||chr(10);
    l_message := l_message || l_msg_tmp;
                        
    -- Detailed info loop
    l_task_no_next := 1;
    for r in (
          select dense_rank() over(order by username, nvl(owb_flow, sql_id), owb_name ) task_no,t.* 
          from monitor_dw.fsess_owb t
          where   snapshot_dt = l_target_dt -- snapshot_dt > sysdate -  2/60/24 --trunc(sysdate, 'MI') = trunc(snapshot_dt, 'MI'
                  -- dont send the same mail again, if it has been sent within the same day  / in the last 3 hours!                           
                  and ("Problem / Action", nvl(to_char(OWB_AUDIT_ID), SQL_ID)) not in ( --and ("Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id)) not in ( -- dont send the same mail again, if it has been sent in the last 3 hours!
                        select nvl("Problem / Action", 'lala'), nvl(to_char(OWB_AUDIT_ID), SQL_ID) --nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id) 
                        from monitor_dw.fsess_owb
                        where 
                            snapshot_dt < t.snapshot_dt
                            AND snapshot_dt > sysdate - 180/60/24 -- trunc(snapshot_dt) = trunc(t.snapshot_dt) 
                  )
          order by username, nvl(owb_flow, sql_id), owb_name, prog          
    ) loop
            if (l_task_no_next = r.task_no) then -- this is the 1st iteration for this task
                l_msg_tmp :=    chr(10)||
                                '****************************'||chr(10)||   
                                '   Details for DW Task No: #'||r.task_no||chr(10)||
                                '****************************'||chr(10);
                l_message := l_message || l_msg_tmp;                                
                l_task_no_next := l_task_no_next + 1;
            end if;
                
            l_msg_tmp :=  
                    '|   ================================================================================'||chr(10)||
                    '|   DB session (sid, serial#, inst_id): '||'('||r.sid||', '||r.serial#||', @'||r.inst_id||')'||chr(10)||
                    '|   ================================================================================'||chr(10)||
                    '|   program:    '||r.prog||chr(10)||                    
                    '|   logon_time: '||to_char(r.logon_time,'dd/mm/yyyy hh24:mi:ss')||chr(10)||
                    '|'||chr(10)||
                    '|   -----------------'||chr(10)||                    
                    '|   Wait Event details'||chr(10)||
                    '|   -----------------'||chr(10)||
                    '|   Wait State:        '||r.wait_state||chr(10)||
                    '|   Wait Class:        '||r.wait_class||chr(10)||
                    '|   Wait Event:        '||r.event||chr(10)||
                    '|   Seconds in Wait:   '||r.secs_in_wait||chr(10)||  
                    '|'||chr(10)||
                    '|   ----------------------------'||chr(10)||                    
                    '|   Object Waiting-for details'||chr(10)||
                    '|   ----------------------------'||chr(10)||
                    '|   obj_owner:     '||r.obj_owner||chr(10)||
                    '|   obj_name:      '||r.obj_name||chr(10)||
                    '|   obj_type:      '||r.obj_type||chr(10)||
                    '|'||chr(10)||
                    '|   -----------------'||chr(10)||                    
                    '|   Blocking Details'||chr(10)||
                    '|   -----------------'||chr(10)||
                    '|   blocking_instance:     '||r.blocking_instance||chr(10)||
                    '|   blocking_session:      '||r.blocking_session||chr(10)||
                    '|   blocker:               '||r.blocker||chr(10)||
                    '|   blocker_owb_node:      '||r.blocker_owb_node||chr(10)||
                    '|   blocker_main_flow:     '||r.blocker_main_flow||chr(10)||
                    '|   kill_blocker_stmnt:    '||r.kill_blocker_stmnt||chr(10)||
                    '|'||chr(10);   
        l_message := l_message || l_msg_tmp;                                              
    end loop;                
    
    -- if there were rows inserted
    if (l_rows_returned = 1) then
        -- send mail
        monitor_dw.fsess_owb_send_mail (l_message);                         
    end if;
    
END fsess_owb_proc;
/

CREATE OR REPLACE procedure monitor_dw.fsess_owb_send_mail (
 msg_in in CLOB --varchar2
)
IS
  l_sender      varchar2(50)    :=  'fdw@ote.gr';
 /* l_recipient1  varchar2(50)    :=  'nkarag@ote.gr';
  l_recipient2  varchar2(50)    :=  'nkarag@ote.gr';
  l_recipient3  varchar2(50)    :=  'nkarag@ote.gr';
  l_recipient4  varchar2(50)    :=  'nkarag@ote.gr';*/
  l_recipient1  varchar2(50)    :=  'StandByBI@ote.gr';
  l_recipient2  varchar2(50)    :=  'a.mantes@neurocom.gr';
  l_recipient2a varchar2(50)    :=  'd.psychogiopoulos@neurocom.gr';  
  l_recipient2b varchar2(50)    :=  'george.papoutsopoulos@oracle.com';  
  l_recipient2c varchar2(50)    :=  'MavrakakisI@unisystems.gr';  
  l_recipient2d varchar2(50)    :=  'TheodorakisJ@unisystems.gr';  
  l_recipient2e varchar2(50)    :=  'l.alexiou@neurocom.gr';  
  l_recipient2f varchar2(50)    :=  'BachourosT@unisystems.gr';  
  l_recipient2g varchar2(50)    :=  'itoperators@ote.gr'; 
  l_recipient2h varchar2(50)    :=  'as_mediation_support@ote.gr'; 
  l_recipient3  varchar2(50)    :=  'fdw@ote.gr';
  l_recipient4  varchar2(50)    :=  'nkarag@ote.gr'; 
  mailhost  CONSTANT VARCHAR2(30) := '10.101.12.40';
  crlf      CONSTANT VARCHAR2(2):= CHR(13) || CHR(10);
--  l_mesg    CLOB; -- VARCHAR2(4000);
  l_subject     varchar2(100)   :=  '*** Problem in DW Running Task  ***';
  mail_conn utl_smtp.connection;
  vrData  RAW(32767);
  herr    varchar2(100); 
  l_times integer;
  l_chunk varchar2(4000);
  l_offset integer;  
begin
  mail_conn := utl_smtp.open_connection(mailhost, 25);
  UTL_smtp.helo(mail_conn, mailhost);
  UTL_smtp.mail(mail_conn, l_sender);
  UTL_smtp.rcpt(mail_conn, l_recipient1);
  UTL_smtp.rcpt(mail_conn, l_recipient2);
  UTL_smtp.rcpt(mail_conn, l_recipient2a);
  UTL_smtp.rcpt(mail_conn, l_recipient2b);
  UTL_smtp.rcpt(mail_conn, l_recipient2c);
  UTL_smtp.rcpt(mail_conn, l_recipient2d);
  UTL_smtp.rcpt(mail_conn, l_recipient2e);
  UTL_smtp.rcpt(mail_conn, l_recipient2f);   
  UTL_smtp.rcpt(mail_conn, l_recipient2g);
  UTL_smtp.rcpt(mail_conn, l_recipient2h);  
  UTL_smtp.rcpt(mail_conn, l_recipient3);    
  UTL_smtp.rcpt(mail_conn, l_recipient4);
    
  --l_mesg := msg_in;
/*
   mesg := 'Date: ' ||
        TO_CHAR( SYSDATE, 'dd Mon yy hh24:mi:ss') || crlf ||
           'From: <'|| pSender ||'>' || crlf ||
           'Subject: '|| pSubject || crlf ||
           'To: '||pRecipient || crlf || '' || crlf || pMessage;
*/           

  UTL_smtp.open_data(mail_conn);
  --dbms_output.put_line('SEND_MAIL_PROC :: Data opened');

  UTL_SMTP.write_data(mail_conn, 'Date: '     || TO_CHAR( SYSDATE, 'dd Mon yy hh24:mi:ss') || UTL_TCP.crlf);
  UTL_SMTP.write_data(mail_conn, 'To: '       || l_recipient1||', ' || l_recipient2 ||', ' || l_recipient2a ||', ' || l_recipient2b ||', ' || l_recipient2c ||', ' || l_recipient2d ||', ' || l_recipient2e ||', ' || l_recipient2f ||', ' || l_recipient2g ||', ' || l_recipient2h
                                              || UTL_TCP.crlf);
  UTL_SMTP.write_data(mail_conn, 'Cc: '       || l_recipient3 || UTL_TCP.crlf);
  UTL_SMTP.write_data(mail_conn, 'Bcc: '       || l_recipient4 || UTL_TCP.crlf);
  UTL_SMTP.write_data(mail_conn, 'From: '     || l_sender || UTL_TCP.crlf);

  UTL_SMTP.write_data(mail_conn, 'Subject: '  || l_subject || UTL_TCP.crlf);
    
  UTL_smtp.write_data(mail_conn, UTL_tcp.CRLF);
  
  /*Actual body is sent here*/  
 -- vrData := utl_raw.cast_to_raw(l_mesg);
 -- UTL_smtp.write_raw_data(mail_conn, vrData);
 
     -- loop in chunks of 4000 characters and compose mail body
     l_times := ceil(dbms_lob.GETLENGTH(msg_in)/4000);
     l_chunk :='';
     l_offset := 1;
     for i IN 1..l_times loop
        l_chunk := dbms_lob.substr(msg_in, 4000, l_offset);
        l_offset := l_offset + 4000; 
        
        -- add chunk to mail body
        vrData := utl_raw.cast_to_raw(l_chunk);
        UTL_smtp.write_raw_data(mail_conn, vrData);
     end loop;
  
  /*Connection is closed here */
  UTL_smtp.close_data(mail_conn);
  UTL_smtp.quit(mail_conn);

 -- msg_status:='Mail was sent withount errors';
  EXCEPTION
  WHEN UTL_smtp.transient_error OR UTL_smtp.permanent_error THEN

    UTL_smtp.quit(mail_conn);
    herr := sqlerrm;
    raise;
    --dbms_output.put_line('SEND_MAIL_PROC :: Error<'||herr||'>');

    --msg_status:='Error while sending Email: Error<'||herr||'>';
  WHEN OTHERS THEN
    UTL_smtp.quit(mail_conn);
    herr := sqlerrm;
    raise;
    --dbms_output.put_line('SEND_MAIL_PROC :: Error<'||herr||'>');
    --msg_status:='Error while sending Email: Error<'||herr||'>';END my_send_mail_gr
END fsess_owb_send_mail;
/


/*

===============================================================
|                  Problem in DW Running Task
|               -------------------------------
|
|   ***Problem / Action***: <problem / action>
|
|   OWB details
|   -----------
|   Main Flow:              <owb_flow>
|   Username:               <username>
|   Node Name:              <owb_name>
|   Node Type:              <owb_type>
|   Duration (mins) p80:    <owb_duration_mins>
|   ***Duration (mins)***:  <duration_mins>
|   Times exceeding p80:
|
|   DB session details
|   -----------
|   inst_id:    <inst_id>
|   sid:        <sid>
|   serial#:    <serial#>
|   logon_time: <login_time>
|
|   SQL details
|   -----------
|   sql_id:     <sql_id>
|   sql_child_number:
|   sql_exec_start:
|   plan_hash_value:
|   sql_text:   
|   Entry PL/SQL Proc:  <entry_plsql_proc>
|
|   Wait Event details
|   -----------------
|   Wait State:
|   Wait Class:
|   Wait Event:
|   Seconds in Wait:    <secs_in_wait>  
|
|   Object Waiting-for details
|   ----------------------------
|   obj_owner:
|   obj_name:
|   obj_type:
|
|   Blocking Details
|   -----------------
|   blocking_instance:
|   blocking_session:
|   blocker:
|   blocker_owb_node:
|   blocker_main_flow:
|   kill_blocker_stmnt:
| 
===============================================================


*/
