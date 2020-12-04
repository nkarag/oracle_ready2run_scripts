SELECT status, count(*), round(ratio_to_report(count(*)) over() *100,2) PCNT
FROM   dba_parallel_execute_chunks
WHERE task_name = 'GATHER_TAB_STATS_IN_PAR'
group by status
/