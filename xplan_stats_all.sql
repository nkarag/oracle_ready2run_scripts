/*
http://datavirtualizer.com/power-of-display_cursor/

The 3 important parts of this query are

Elapsed is per row source, not cumulative of it’s children
LIO_RATIO
TCP_GRAPH

Elapsed time:
	format has a huge drawback in the display_cursor output as each lines elapsed time includes the elapsed time of all the children 
	which makes an execution plan difficult to scan and see where the time is being spent. In the above output the elapsed time represents 
	the elapsed time of each row source line.

LIO_RATIO:
	shows the number of buffers accessed per row returned. Ideally 1 buffer or less is accessed per row returned. When the number of buffers 
	per row becomes large, it’s a good indication that there is a more optimal method to get the rows.  The I/O stats include the stats of the child row source, 
	so the query has to get the I/O from the childern and subtract from the parent, making the query a bit more complex.

TCP_GRAPH:
	graphically shows the ratio of estimated rows to actual rows. The estimated rows used is cardinality* starts, not just cardinality. 
	This value can be compared directly to actual_rows and the difference in order of magnitude is shown. Each ‘+’ represents and order of magnitude 
	larger and each “-” represents an order of magnitude smaller. The more orders of magnitude, either way, the more the optimizers calculations are off and 
	thus like more pointing to a possible plan that is suboptimal.
*/
@sqlplus_settings_store

col cn format 99
col ratio format 99
col ratio1 format A6
--set pagesize 1000
set linesize 140
break on sql_id on cn
col lio_rw format 999
col "operation" format a150
col a_rows for 999,999,999
col e_rows for 999,999,999
col elapsed for 999,999,999
col TCF_GRAPH for a10

Def v_sql_id=&SQL_ID
Def v_child_number=&CHILD_NUMBER

select
		operation_id,
       -- sql_id,
       --hv,
       childn                                         cn,
       --ptime, stime,
       case when stime - nvl(ptime ,0) > 0 then
          stime - nvl(ptime ,0)
        else 0 end as elapsed_secs,
       nvl(trunc((lio-nvl(plio,0))/nullif(a_rows,0)),0) lio_ratio,
       --id,
       --parent_id,
       --starts,
       --nvl(ratio,0)                                    TCF_ratio,
       ' '||case when ratio > 0 then
                rpad('-',ratio,'-')
             else
               rpad('+',ratio*-1 ,'+')
       end as                                           TCF_GRAPH,
       starts*cardinality                              e_rows,
                                                       a_rows,
       --nvl(lio,0) lio, nvl(plio,0)                      parent_lio,
                                                         "operation"
from (
  SELECT
	 stats.id												operation_id,
      round(stats.LAST_ELAPSED_TIME/1e6)                            stime,
      round(p.elapsed/1e6)                                  ptime,
      stats.sql_id                                        sql_id
    , stats.HASH_VALUE                                    hv
    , stats.CHILD_NUMBER                                  childn
    , to_char(stats.id,'990')
      ||decode(stats.access_predicates,null,null,'A')
      ||decode(stats.filter_predicates,null,null,'F')     id
    , stats.parent_id
    , stats.CARDINALITY                                    cardinality
    , LPAD(' ',depth)||stats.OPERATION||' '||
      stats.OPTIONS||' '||
      stats.OBJECT_NAME||
      DECODE(stats.PARTITION_START,NULL,' ',':')||
      TRANSLATE(stats.PARTITION_START,'(NRUMBE','(NR')||
      DECODE(stats.PARTITION_STOP,NULL,' ','-')||
      TRANSLATE(stats.PARTITION_STOP,'(NRUMBE','(NR')      "operation",
      stats.last_starts                                     starts,
      stats.last_output_rows                                a_rows,
      (stats.last_cu_buffer_gets+stats.last_cr_buffer_gets) lio,
      p.lio                                                 plio,
      trunc(log(10,nullif
         (stats.last_starts*stats.cardinality/
          nullif(stats.last_output_rows,0),0)))             ratio
  FROM
       v$sql_plan_statistics_all stats
       , (select sum(last_cu_buffer_gets + last_cr_buffer_gets) lio,
                 sum(LAST_ELAPSED_TIME) elapsed,
                 child_number,
                 parent_id,
                 sql_id
         from v$sql_plan_statistics_all
         group by child_number,sql_id, parent_id) p
  WHERE
    stats.sql_id='&v_sql_id'  and stats.child_number = '&v_child_number' and
    p.sql_id(+) = stats.sql_id and
    p.child_number(+) = stats.child_number and
    p.parent_id(+)=stats.id
)
order by sql_id, childn , id
/
@sqlplus_get_settings