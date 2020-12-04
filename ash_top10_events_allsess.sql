-- Find top wait events from all session sampled in a specidief period of time
set pagesize 999
set lines 999
col wait_class format a13
col event format a50

select *
from (
select 
decode(session_state, 'ON CPU', 'ON CPU', wait_class) wait_class_or_CPU, --WAIT_CLASS, 
EVENT, round(ratio_to_report(count(*)) over() *100) PCNT, count(*) no_samples, 
count(distinct SESSION_ID) no_session_ids,
(max(TIME_WAITED)/100)/100 max_time_waited_mins --, P1TEXT, P2TEXT, P3TEXT 
from gv$active_session_history a 
where 
((session_state = 'WAITING' and WAIT_TIME = 0) or session_state ='ON CPU')
--session_state= 'WAITING' and WAIT_TIME = 0 -- choose only sessions that are waiting at the time of the sampling
and SAMPLE_TIME > sysdate - (&minutes_from_now/(24*60))
group by 
decode(session_state, 'ON CPU', 'ON CPU', wait_class), --WAIT_CLASS, 
EVENT --, P1TEXT, P2TEXT, P3TEXT
order by count(*) desc
)
where rownum < 11
/
