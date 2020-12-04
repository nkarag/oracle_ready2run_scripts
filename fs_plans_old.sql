prompt Find the available execution plans for a specific SQL_ID

undef sql_id
WITH
p AS (
SELECT t1.plan_hash_value, t2.cost
  FROM gv$sql_plan t1 join 
			(	select inst_id, plan_hash_value, cost 
				from gv$sql_plan_statistics_all 
				WHERE sql_id = TRIM('&&sql_id.') AND depth =0
			) t2 on(t1.inst_id=t2.inst_id and t1.plan_hash_value=t2.plan_hash_value)
 WHERE sql_id = TRIM('&&sql_id.')
   AND other_xml IS NOT NULL
 UNION
SELECT plan_hash_value, connect_by_root cost
  FROM dba_hist_sql_plan
 WHERE sql_id = TRIM('&&sql_id.')
   AND other_xml IS NOT NULL 
   start with depth = 0
   connect by prior id = parent_id),
m AS (
SELECT plan_hash_value,
       SUM(elapsed_time)/SUM(executions) avg_et_secs
  FROM gv$sql
 WHERE sql_id = TRIM('&&sql_id.')
   AND executions > 0
 GROUP BY
       plan_hash_value ),
a AS (
SELECT plan_hash_value,
       SUM(elapsed_time_total)/SUM(executions_total) avg_et_secs 
  FROM dba_hist_sqlstat
 WHERE sql_id = TRIM('&&sql_id.')
   AND executions_total > 0
 GROUP BY
       plan_hash_value )
SELECT p.plan_hash_value,
       ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3) avg_et_secs, p.cost cost
  FROM p, m, a
 WHERE p.plan_hash_value = m.plan_hash_value(+)
   AND p.plan_hash_value = a.plan_hash_value(+)
 ORDER BY
       cost, avg_et_secs NULLS LAST;
/
undef sql_id