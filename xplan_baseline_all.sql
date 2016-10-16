set linesize 9999
set pagesize 999
--set serveroutput off

select * from table( dbms_xplan.DISPLAY_SQL_PLAN_BASELINE(sql_handle =>'&sql_handle', plan_name => '&plan_name', format=>'ALL'));