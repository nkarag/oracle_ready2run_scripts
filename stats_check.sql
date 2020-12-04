set linesize 999
col JOB_NAME format a25
col JOB_DURATION format a13
col JOB_ERROR format 999
col JOB_INFO format a70 trunc
col job_status format a10

select JOB_NAME, JOB_STATUS, JOB_START_TIME, JOB_DURATION, JOB_ERROR, JOB_INFO 
from DBA_AUTOTASK_JOB_HISTORY  
where client_name='auto optimizer stats collection'  
order by window_start_time desc;