column value for 999,999
select decode(INST_ID, null, 'TOTAL', INST_ID) as INSTANCE_ID, STATISTIC, sum(VALUE)
from GV$PX_PROCESS_SYSSTAT 
where statistic like '%In Use%'
group by rollup(INST_ID), STATISTIC;
