set linesize 9999
set pagesize 999


WITH v AS (
SELECT /*+ MATERIALIZE */
       DISTINCT sql_id, inst_id, child_number
  FROM gv$sql
 WHERE sql_id = '&sql_id' and child_number = '&child_number'
   AND loaded_versions > 0
 ORDER BY 1, 2, 3 )
SELECT /*+ ORDERED USE_NL(t) */
       RPAD('Inst: '||v.inst_id, 9)||' '||RPAD('Child: '||v.child_number, 11) inst_child, 
       t.plan_table_output
  FROM v, TABLE(DBMS_XPLAN.DISPLAY('gv$sql_plan_statistics_all', NULL, 'ALL ALLSTATS LAST -PROJECTION',  --'ALLSTATS LAST alias partition cost', 
       'inst_id = '||v.inst_id||' AND sql_id = '''||v.sql_id||''' AND child_number = '||v.child_number)) t
	   
/