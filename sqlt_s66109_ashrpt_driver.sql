REM $Header: 215187.1 sqlt_s66109_ashrpt_driver.sql 11.4.5.9 2013/07/15 carlos.sierra $
VAR dbid         NUMBER;
VAR inst_num     NUMBER;
VAR btime        VARCHAR2(14);
VAR etime        VARCHAR2(14);
VAR options      NUMBER;
VAR slot_width   NUMBER;
VAR sid          NUMBER;
VAR sql_id       VARCHAR2(13);
VAR wait_class   VARCHAR2(64);
VAR service_hash NUMBER;
VAR module       VARCHAR2(64);
VAR action       VARCHAR2(64);
VAR client_id    VARCHAR2(64);
VAR plsql_entry  VARCHAR2(64);
VAR data_src     NUMBER;
EXEC :dbid := 604944182;
EXEC :options := 0;
EXEC :slot_width := 0;
EXEC :sid := NULL;
EXEC :sql_id := 'afd89vyk8j5mm';
EXEC :wait_class := NULL;
EXEC :service_hash := NULL;
EXEC :module := NULL;
EXEC :action := NULL;
EXEC :client_id := NULL;
EXEC :plsql_entry := NULL;
SET ECHO OFF FEED OFF VER OFF SHOW OFF HEA OFF LIN 2000 NEWP NONE PAGES 0 LONG 2000000 LONGC 2000 SQLC MIX TAB ON TRIMS ON TI OFF TIMI OFF ARRAY 100 NUMF "" SQLP SQL> SUF sql BLO . RECSEP OFF APPI OFF AUTOT OFF;
EXEC :data_src := 1;
EXEC :inst_num := 2;
EXEC :btime := '20130715162935';
EXEC :etime := '20130715162935';
SET TERM ON;
PRO ... generating sqlt_s66109_ashrpt_0001_mem_2_0715_1629.html ...
SET TERM OFF;
SPO sqlt_s66109_ashrpt_0001_mem_2_0715_1629.html;
SELECT '<!-- '||TO_CHAR(SYSDATE, 'YYYY-MM-DD/HH24:MI:SS')||' -->' FROM dual;
SELECT column_value FROM TABLE(sqltxadmin.sqlt$a.ash_report_html_11(:dbid, :inst_num, TO_DATE(:btime, 'YYYYMMDDHH24MISS'), TO_DATE(:etime, 'YYYYMMDDHH24MISS'), :options, :slot_width, :sid, :sql_id, :wait_class, :service_hash, :module, :action, :client_id, :plsql_entry, :data_src));
SELECT '<!-- '||TO_CHAR(SYSDATE, 'YYYY-MM-DD/HH24:MI:SS')||' -->' FROM dual;
SPO OFF;
EXEC :inst_num := 4;
EXEC :btime := '20130715162935';
EXEC :etime := '20130715162935';
SET TERM ON;
PRO ... generating sqlt_s66109_ashrpt_0002_mem_4_0715_1629.html ...
SET TERM OFF;
SPO sqlt_s66109_ashrpt_0002_mem_4_0715_1629.html;
SELECT '<!-- '||TO_CHAR(SYSDATE, 'YYYY-MM-DD/HH24:MI:SS')||' -->' FROM dual;
SELECT column_value FROM TABLE(sqltxadmin.sqlt$a.ash_report_html_11(:dbid, :inst_num, TO_DATE(:btime, 'YYYYMMDDHH24MISS'), TO_DATE(:etime, 'YYYYMMDDHH24MISS'), :options, :slot_width, :sid, :sql_id, :wait_class, :service_hash, :module, :action, :client_id, :plsql_entry, :data_src));
SELECT '<!-- '||TO_CHAR(SYSDATE, 'YYYY-MM-DD/HH24:MI:SS')||' -->' FROM dual;
SPO OFF;
EXEC :data_src := 2;
--
HOS zip -m sqlt_s66109_ashrpt_0002 sqlt_s66109_ashrpt_*.html
--
HOS zip -m sqlt_s66109_driver sqlt_s66109_ashrpt_driver.sql
