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
    l_owb_node_name     varchar2(100); 
    l_days_back         pls_integer;
    l_prfrmnce_thrshld  pls_integer;
    l_mins_on_wait_thrshld  pls_integer;
    l_message           CLOB; --varchar2(4000);
    l_rows_returned     number;
    l_cnt               number;
BEGIN
    -- check if monthly flows are running
    select      CASE    WHEN   (select run_date from stage_dw.dw_control_table where procedure_name = 'LAST_RUN_END_TIME') < sysdate  -- LEVEL0 is not running 
                            AND last_day((select run_date from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')) = (select run_date /*date'2015-08-31'*/ from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN') 
                          THEN    1
                        WHEN    (select run_date from stage_dw.dw_control_table where procedure_name = 'LAST_RUN_END_TIME') > sysdate -- LEVEL0 is running
                            AND last_day((select run_date+1 from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')) = (select run_date+1 /*date'2015-08-31'*/ from stage_dw.dw_control_table where procedure_name = 'OTE_DW_LAST_RUN')
                          THEN 1
                        ELSE 0
                END monthly_flow_ind into l_monthly_flow_ind
    from dual;     
    
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
        case    when    owb_type <> 'ProcessFlow' AND nullif(duration_mins_p80,0) < 5 AND duration_mins > 30
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer') -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- medium tasks                                
                when    owb_type <> 'ProcessFlow' AND nullif(duration_mins_p80,0) between 5 AND 15 AND duration_mins > 50 
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer') -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- heavy tasks                      
                when    owb_type <> 'ProcessFlow' AND nullif(duration_mins_p80,0) > 15 AND duration_mins > 60 
                        AND round(duration_mins/nullif(duration_mins_p80,0),1) >= nvl(l_prfrmnce_thrshld, 3)
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer')  -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- non OWB tasks                    
                when    nvl(owb_type, 'xx') <> 'ProcessFlow' AND ((duration_mins_p80 IS NULL) or (duration_mins_p80 = 0)) 
                        AND nvl(duration_mins, round((sysdate - sql_exec_start)* 24 * 60,1)) > 120 
                        AND NOT(state = 'WAITING' AND event = 'PL/SQL lock timer')  -- exclude waiting tasks
                    THEN  '"LOW PERFORMANCE"'
                -- tasks sleeping                    
                when    owb_type <> 'ProcessFlow'  AND duration_mins > 60
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
                            AND wait_class not in ('User I/O', 'Idle', 'Network') 
                            AND round(WAIT_TIME_MICRO/1e6) > 60* nvl(l_mins_on_wait_thrshld, 30) then '- "PROBLEMATIC WAIT FOR TOO LONG" / "CALL DBA"'  
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
                                ) > 60* nvl(l_mins_on_wait_thrshld, 30) 
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
    )
    select sysdate, t.*
    from ff t
    where 1=1
         AND "Problem / Action" IS NOT NULL;
         
    commit;
    
    -- send mail - only for the rows just inserted
    l_rows_returned := 0;
    l_cnt := 0;
    l_message := '';
    for r in (
        select * 
        from monitor_dw.fsess_owb 
        where snapshot_dt > sysdate - 2/60/24 --trunc(sysdate, 'MI') = trunc(snapshot_dt, 'MI')
        order by username, owb_flow, owb_name 
    )
    loop
        l_rows_returned := 1;
        l_cnt := l_cnt + 1;
            -- build message body
        l_message := l_message ||   
                    chr(10)||
                    '==============================================================='||chr(10)||
                    '|                  Problem in DW Running Task (#'||l_cnt||')'||chr(10)||
                    '|                -----------------------------------------'||chr(10)||
                    '|'||chr(10)||
                    '|   ***Problem / Action***:    '||r."Problem / Action"||chr(10)||
                    '|'||chr(10)||
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
                    '|   DB session details'||chr(10)||
                    '|   -----------'||chr(10)||
                    '|   inst_id:    '||r.inst_id||chr(10)||
                    '|   sid:        '||r.sid||chr(10)||
                    '|   serial#:    '||r.serial#||chr(10)||
                    '|   program:    '||r.prog||chr(10)||                    
                    '|   logon_time: '||to_char(r.logon_time,'dd/mm/yyyy hh24:mi:ss')||chr(10)||
                    '|'||chr(10)||
                    '|   -----------'||chr(10)||                    
                    '|   SQL details'||chr(10)||
                    '|   -----------'||chr(10)||
                    '|   sql_id:            '||r.sql_id||chr(10)||
                    '|   sql_child_number:  '||r.sql_child_number||chr(10)||
                    '|   plan_hash_value:   '||r.plan_hash_value||chr(10)||                    
                    '|   sql_exec_start:    '||to_char(r.sql_exec_start,'dd/mm/yyyy hh24:mi:ss')||chr(10)||
                    '|   sql_text:          '||substr(r.sql_text,1,120)||chr(10)||   
                    '|   Entry PL/SQL Proc:     '||r.entry_plsql_proc||chr(10)||
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
                    '|'||chr(10)|| 
                    '==============================================================='||chr(10);          
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
  l_recipient3  varchar2(50)    :=  'fdw@ote.gr';
  l_recipient4  varchar2(50)    :=  'nkarag@ote.gr';  
  mailhost  CONSTANT VARCHAR2(30) := '10.101.12.40';
  crlf      CONSTANT VARCHAR2(2):= CHR(13) || CHR(10);
  l_mesg    CLOB; -- VARCHAR2(4000);
  l_subject     varchar2(100)   :=  '*** Problem in DW Running Task  ***';
  mail_conn utl_smtp.connection;
  vrData  RAW(32767);
  herr    varchar2(100); 
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
  UTL_smtp.rcpt(mail_conn, l_recipient3);    
  UTL_smtp.rcpt(mail_conn, l_recipient4);
    
  l_mesg := msg_in;
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
  UTL_SMTP.write_data(mail_conn, 'To: '       || l_recipient1||', ' || l_recipient2 ||', ' || l_recipient2a ||', ' || l_recipient2b ||', ' || l_recipient2c ||', ' || l_recipient2d ||', ' || l_recipient2e ||', ' || l_recipient2f 
                                              || UTL_TCP.crlf);
  UTL_SMTP.write_data(mail_conn, 'Cc: '       || l_recipient3 || UTL_TCP.crlf);
  UTL_SMTP.write_data(mail_conn, 'Bcc: '       || l_recipient4 || UTL_TCP.crlf);
  UTL_SMTP.write_data(mail_conn, 'From: '     || l_sender || UTL_TCP.crlf);

  UTL_SMTP.write_data(mail_conn, 'Subject: '  || l_subject || UTL_TCP.crlf);
    
  UTL_smtp.write_data(mail_conn, UTL_tcp.CRLF);
  /*Actual body is sent here*/
  
  vrData := utl_raw.cast_to_raw(l_mesg);
  UTL_smtp.write_raw_data(mail_conn, vrData);
  
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
