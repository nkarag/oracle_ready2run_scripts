col name                        format a12            heading "Name" 
col active_sessions             format 999            heading "Active|Sessions" 
col execution_waiters           format 999            heading "Execution|Waiters" 
col requests                    format 9,999,999      heading "Requests" 
col cpu_wait_time               format 999,999,999    heading "CPU Wait|Time" 
col cpu_waits                   format 99,999,999     heading "CPU|Waits" 
col consumed_cpu_time           format 99,999,999     heading "Consumed|CPU Time" 
col yields                      format 9,999,999      heading "Yields" 
 
SELECT DECODE(name, '_ORACLE_BACKGROUND_GROUP_', 'BACKGROUND', name) name, 
       active_sessions, execution_waiters, requests,  
       cpu_wait_time, cpu_waits, consumed_cpu_time, yields 
  FROM gv$rsrc_consumer_group 
ORDER BY cpu_wait_time; 