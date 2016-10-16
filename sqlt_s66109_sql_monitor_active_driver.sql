REM $Header: 215187.1 sqlt_s66109_sql_monitor_active_driver.sql 11.4.5.9 2013/07/15 carlos.sierra $
VAR mon_exec_start VARCHAR2(14);
VAR mon_exec_id NUMBER;
VAR mon_sql_plan_hash_value NUMBER;
VAR mon_inst_id NUMBER;
VAR mon_report CLOB;
VAR mon_sql_id VARCHAR2(13);
EXEC :mon_sql_id := 'afd89vyk8j5mm';
SET ECHO OFF FEED OFF VER OFF SHOW OFF HEA OFF LIN 2000 NEWP NONE PAGES 0 LONG 2000000 LONGC 2000 SQLC MIX TAB ON TRIMS ON TI OFF TIMI OFF ARRAY 100 NUMF "" SQLP SQL> SUF sql BLO . RECSEP OFF APPI OFF AUTOT OFF;
EXEC :mon_exec_start := '20130715162934';
EXEC :mon_exec_id := 33554432;
EXEC :mon_sql_plan_hash_value := 2898002369;
EXEC :mon_inst_id := 2;
SET TERM ON;
PRO ... generating sqlt_s66109_sql_monitor_active_33554432_2898002369_2.html ...
SET TERM OFF;
SPO sqlt_s66109_sql_monitor_active_33554432_2898002369_2.html;
SELECT '<!-- '||TO_CHAR(SYSDATE, 'YYYY-MM-DD/HH24:MI:SS')||' -->' FROM dual;
PRO <!-- begin SYS.DBMS_SQLTUNE.REPORT_SQL_MONITOR
BEGIN
  :mon_report := sqltxadmin.sqlt$a.report_sql_monitor (
    p_sql_id         => :mon_sql_id,
    p_sql_exec_start => TO_DATE(:mon_exec_start, 'YYYYMMDDHH24MISS'),
    p_sql_exec_id    => :mon_exec_id,
    p_report_level   => 'ALL',
    p_type           => 'ACTIVE' );
END;
/
PRO end -->
SELECT '<!-- '||TO_CHAR(SYSDATE, 'YYYY-MM-DD/HH24:MI:SS')||' -->' FROM dual;
SELECT :mon_report FROM DUAL;
SELECT '<!-- '||TO_CHAR(SYSDATE, 'YYYY-MM-DD/HH24:MI:SS')||' -->' FROM dual;
SPO OFF;
--
HOS zip -m sqlt_s66109_sql_monitor_active_0001 sqlt_s66109_sql_monitor_active_*.html
--
HOS zip -m sqlt_s66109_driver sqlt_s66109_sql_monitor_active_driver.sql
