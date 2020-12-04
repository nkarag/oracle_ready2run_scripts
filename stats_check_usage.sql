col OWNER format a10
col JOB_NAME format a30
col RUN_DURATION format a13
col status format a10
col actual_start_date format a45

-- check duration of concurrent stats execution job after successful completion 
select owner, job_name, job_subname, status, actual_start_date, run_duration
from ALL_SCHEDULER_JOB_RUN_DETAILS
where
 owner = 'DWADMIN' and job_name like '%CHAIN'
 and status = 'SUCCEEDED' 
order by actual_start_date desc
/
   
--select owner, job_name, status, actual_start_date, run_duration
--from ALL_SCHEDULER_JOB_RUN_DETAILS
--where
 --owner = 'DWADMIN' and job_name like 'ST%'
 --and status = 'SUCCEEDED' 
--order by actual_start_date desc
--/