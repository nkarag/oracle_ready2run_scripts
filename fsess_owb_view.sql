
---------------------- JOB ------------------------
BEGIN
sys.dbms_scheduler.create_job( 
job_name => '"MONITOR_DW"."FSESS_OWB_JOB"',
job_type => 'PLSQL_BLOCK',
job_action => 'begin

insert /*+ append */ into monitor_dw.fsess_owb
select *
from monitor_dw.v_fsess_owb
where "Problem / Action" IS NOT NULL;

commit;
end;',
repeat_interval => 'FREQ=MINUTELY;INTERVAL=30',
start_date => systimestamp at time zone 'Europe/Athens',
job_class => '"DEFAULT_JOB_CLASS"',
comments => 'run fsess_owb every 30 minutes',
auto_drop => FALSE,
enabled => TRUE);
END;
--------------

select *
from dba_scheduler_running_jobs
where job_name =  'FSESS_OWB_JOB';

select *
from dba_scheduler_jobs
where job_name =  'FSESS_OWB_JOB'

-- stop the job when running
exec dbms_scheduler.stop_job(job_name => 'MONITOR_DW.FSESS_OWB_JOB', forct => TRUE)

-- disable the job
exec DBMS_SCHEDULER.DISABLE (job_name => 'MONITOR_DW.FSESS_OWB_JOB', forct => TRUE)


-- drop the job
exec dbms_scheduler.drop_job(job_name => 'MONITOR_DW.FSESS_OWB_JOB')
   

--------------------- TABLE ------------------------
create table monitor_dw.fsess_owb
as
select *
from monitor_dw.v_fsess_owb
where "Problem / Action" IS NOT NULL
 
insert /*+ append */ into monitor_dw.fsess_owb
select *
from monitor_dw.v_fsess_owb
where "Problem / Action" IS NOT NULL;

commit;

select * from monitor_dw.fsess_owb;

--------------------- VIEW -------------------------------
select *
from monitor_dw.v_fsess_owb;


create or replace view monitor_dw.v_fsess_owb
as
with owb_execs
as ( -- owb executions of interest
    select  /*+ qb_name(owb_execs) materialize no_merge   */ execution_audit_id owb_audit_id,
            execution_name,
            SUBSTR (execution_name, INSTR (execution_name, ':') + 1) owb_name,
            (select execution_name from owbsys.all_rt_audit_executions where execution_audit_id = t1.top_level_execution_audit_id) owb_flow,
            DECODE (task_type,'PLSQL', 'Mapping','PLSQLProcedure', 'Procedure', 'PLSQLFunction', 'Function', task_type) owb_type,
            execution_audit_status owb_status,
            return_result owb_result,
            created_on owb_created_on,
            updated_on owb_updated_on,
            ROUND ( (case when updated_on = created_on then sysdate else updated_on end - created_on) * 24 * 60, 1) duration_mins,
            PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY ROUND ( (updated_on - created_on) * 24 * 60, 1) ASC) OVER (partition by execution_name) 
                duration_mins_p80            
    from owbsys.all_rt_audit_executions t1 
    where 1=1
        --AND EXECUTION_AUDIT_STATUS = 'BUSY' -- only running nodes
        AND SUBSTR (execution_name, INSTR (execution_name, ':') + 1) = nvl(upper(trim('&&owb_node_name')), SUBSTR (execution_name, INSTR (execution_name, ':') + 1))
        AND created_on > sysdate - nvl('&&days_back', case when &&mon_flow = 0 then 15 else 100 end)  -- for checking execution time in history
        AND to_char(created_on, 'DD') = case when &&mon_flow = 1 then '01' else to_char(created_on, 'DD') end 
        --AND created_on > sysdate - 1 -- for performance reasons!
        AND nvl(return_result,'OK') = 'OK' -- take only running nodes, or succesfullly completed 
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
)  
select  /*+  qb_name(main)
             PUSH_SUBQ(@monthly_flow)
             PUSH_SUBQ(@owb_execs) 
             PUSH_SUBQ(@owb_sessions)
         */
         sysdate snapshot_dt,
        --*** Diagnosis
            -- Low Performance
        case    when owb_type <> 'ProcessFlow' AND nullif(duration_mins_p80,0) < 5 AND duration_mins > 30 THEN  '"LOW PERFORMANCE"'
                when owb_type <> 'ProcessFlow' AND nullif(duration_mins_p80,0) between 5 AND 15 AND duration_mins > 50 THEN  '"LOW PERFORMANCE"'  
                when owb_type <> 'ProcessFlow' AND nullif(duration_mins_p80,0) > 15 AND duration_mins > 60 
                    AND round(duration_mins/nullif(duration_mins_p80,0),1) >= nvl('&&prfrmnce_thrshld', 3)  THEN  '"LOW PERFORMANCE"'
                when nvl(owb_type, 'xx') <> 'ProcessFlow' AND ((duration_mins_p80 IS NULL) or (duration_mins_p80 = 0)) 
                    AND nvl(duration_mins, round((sysdate - logon_time)* 24 * 60,1)) > 120 THEN  '"LOW PERFORMANCE"'                                 
/*        when   round(duration_mins/nullif(duration_mins_p80,0),1) >= nvl('&&prfrmnce_thrshld', 2) 
                    AND duration_mins > 60 then '"LOW PERFORMANCE"'*/
             else null
        end
        ||
            -- Waiting on an "non-working" wait event
        case when   state = 'WAITING' 
                    AND wait_class not in ('User I/O', 'Idle', 'Network') 
                    AND round(WAIT_TIME_MICRO/1e6) > 60* nvl('&&mins_on_wait_thrshld', 30) then '- "PROBLEMATIC WAIT FOR TOO LONG" / "CALL DBA"'  
             else null
        end         
        ||
            -- Session blocked by another session
        case when   blocking_session is not null
                        -- not blocked by another parallel slave of the same query 
                    AND sql_id <> (select sql_id from gv$session where inst_id = f.blocking_instance and sid = f.blocking_session)
                        -- blocked for more than  mins_on_wait_thrshld
                        -- get waited time for v$session_event and not for v$session, in order to catch cases where a blocked call is looping and thus the waited time is instanteneous but over all is significant.
                    AND (   select (TIME_WAITED/100) TIME_WAITED_SECS 
                            from gv$session_event 
                            where inst_id = f.inst_id and  sid = f.sid 
                                  and wait_class in ('Application') 
                        ) > 60* nvl('&&mins_on_wait_thrshld', 30) 
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
order by owb_flow, username, owb_name
/