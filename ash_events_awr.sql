set pagesize 999
set lines 999

prompt  *****************
prompt Analysis of wait events for the last N hours from a timepoint T for a specific sql_id from dba_hist_active_sess_history
prompt Enter a timepoint in 'DD-MM-YYYY HH24:MI:SS' format and how many hours before this you want to go (press enter fo all samples)
prompt  *****************
prompt

select event, decode(session_state, 'ON CPU', 'ON CPU', wait_class) wait_class_or_CPU,round(ratio_to_report(count(*)) over() *100) PCNT, owner, object_name, object_type, count(*) nosamples, cnttot nosamplestot, P1TEXT, P2TEXT, P3TEXT 
    from (
		select    event, wait_class, P1TEXT, P2TEXT, P3TEXT, owner, object_name, object_type, session_state,  count(*) over() cnttot
		from dba_hist_active_sess_history a  left join dba_objects b on(a.CURRENT_OBJ# = b.object_id)
		where  
		SAMPLE_TIME >= nvl(to_date('&timepoint', 'DD-MM-YYYY HH24:MI:SS'),SAMPLE_TIME)  - (nvl('&hours_from_timepoint',0)/24) and
		((session_state = 'WAITING' and WAIT_TIME = 0) or session_state ='ON CPU')
		and sql_id = nvl('&sql_id',sql_id)
		and SQL_CHILD_NUMBER = nvl('&SQL_CHILD_NUMBER',0)
    )t
    group by event, wait_class, session_state, P1TEXT, P2TEXT, P3TEXT, owner, object_name, object_type, cnttot
    order by pcnt desc
/