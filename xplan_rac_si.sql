---------------------------------------------------------------------------------------
--	xplan_rac_si.sql
---------------------------------------------------------------------------------------
set linesize 9999
set pagesize 999

SELECT *
  FROM TABLE(DBMS_XPLAN.DISPLAY('gv$sql_plan_statistics_all', NULL, 'ALL ALLSTATS LAST -PROJECTION', 
       ' sql_id = '''||'&sql_id'||''' AND child_number = '||'&child_number')) t
/	   
