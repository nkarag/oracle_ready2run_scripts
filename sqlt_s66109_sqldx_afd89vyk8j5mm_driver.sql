REM $Header: 1366133.1 sqlt_s66109_sqldx_afd89vyk8j5mm_driver.sql 11.4.5.9 2013/06/10 carlos.sierra $
REM created by sqldx.sql
SET DEF ON;
SET DEF ^ TERM OFF ECHO OFF FEED OFF VER OFF SHOW OFF HEA OFF LIN 2000 NUM 20 NEWP NONE PAGES 0 LONG 2000000 LONGC 2000 SQLC MIX TAB ON TRIMS ON TI OFF TIMI OFF ARRAY 100 NUMF "" SQLP SQL> SUF sql BLO . RECSEP OFF APPI OFF AUTOT OFF SERVEROUT ON SIZE UNL;
ALTER SESSION SET nls_numeric_characters = ".,";
ALTER SESSION SET nls_date_format = 'YYYY-MM-DD/HH24:MI:SS';
ALTER SESSION SET nls_timestamp_format = 'YYYY-MM-DD/HH24:MI:SS.FF';
ALTER SESSION SET nls_timestamp_tz_format = 'YYYY-MM-DD/HH24:MI:SS.FF TZH:TZM';
CL BRE COL;
-- YYYY-MM-DD/HH24:MI:SS
COL time_stamp1 NEW_V time_stamp1 FOR A20;
/*********************************************************************************/
ERROR:
ORA-03114: not connected to ORACLE


DECLARE
*
ERROR at line 1:
ORA-00028: your session has been killed
ORA-00028: your session has been killed
ORA-06512: at "SYS.LOGSTDBY$TABF", line 59
ORA-06512: at line 1
ORA-06512: at line 549


SET TERM ON ECHO OFF FEED 6 VER ON SHOW OFF HEA ON LIN 80 NUM 10 NEWP 1 PAGES 14 LONG 80 LONGC 80 SQLC MIX TAB ON TRIMS OFF TI OFF TIMI OFF ARRAY 15 NUMF "" SQLP SQL> SUF sql BLO . RECSEP WR APPI OFF SERVEROUT OFF AUTOT OFF;
PRO
PRO sqlt_s66109_sqldx_*.zip files have been created.
SET DEF ON;
