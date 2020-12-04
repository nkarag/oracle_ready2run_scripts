alter session set nls_date_format = 'dd/mm/yyyy hh24:mi:ss'

select  snapshot_dt,
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
        listagg('('||inst_id||', '||sid||', '||serial#||', '||prog||','||to_char(logon_time, 'hh24:mi:ss')||')', chr(10)
            ) within group (order by prog) session_details,        
        count(*) num_of_sessions,
        sql_id,
        sql_child_number,
        to_char(sql_exec_start, 'dd/mm/yyyy hh24:mi:ss') sql_exec_start,
        sql_text
       -- entry_PLSQL_Proc          
    from monitor_dw.fsess_owb t
    where snapshot_dt > sysdate - 1 -- 2/60/24 --trunc(sysdate, 'MI') = trunc(snapshot_dt, 'MI'                           
        and ("Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id)) not in ( -- dont send the same mail again, if it has been sent in the last 3 hours!
            select "Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id) 
            from monitor_dw.fsess_owb
            where 
                snapshot_dt < t.snapshot_dt
                AND  snapshot_dt > sysdate - 180/60/24
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
        sql_exec_start,
        sql_text
       -- entry_plsql_proc                
order by username, owb_flow, owb_name--, prog     


 select * 
                from monitor_dw.fsess_owb t
                where snapshot_dt > sysdate - 1 --2/60/24 --trunc(sysdate, 'MI') = trunc(snapshot_dt, 'MI'                           
                    and ("Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id)) not in ( -- dont send the same mail again, if it has been sent in the last 3 hours!
                        select "Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id) 
                        from monitor_dw.fsess_owb
                        where 
                            snapshot_dt < t.snapshot_dt
                            AND  snapshot_dt > sysdate - 180/60/24
                    )
          order by username, owb_flow, owb_name, prog     

select to_char(sysdate, 'dd/mm/yyyy hh24:mi:ss') from dual

to_char(logon_time, 'dd/mm/yyyy hh24:mi:ss')


select * 
                from monitor_dw.fsess_owb t
                where snapshot_dt > sysdate -  30/60/24 --trunc(sysdate, 'MI') = trunc(snapshot_dt, 'MI'                           
                    and ("Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id)) not in ( -- dont send the same mail again, if it has been sent in the last 3 hours!
                        select "Problem / Action", nvl(OWB_FLOW, username), nvl(OWB_NAME, sql_id) 
                        from monitor_dw.fsess_owb
                        where 
                            snapshot_dt < t.snapshot_dt
                            AND  snapshot_dt > sysdate - 180/60/24
                    )
order by username, owb_flow, owb_name, prog 
          
          
          select sysdate - 180/60/24 from dual;