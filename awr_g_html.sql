accept snap_start prompt 'enter value for start snapshot id:'
accept snap_end prompt 'enter value for end snapshot id:'
accept db_id prompt 'enter value for DBID:'

-- save sqlplus settings 
set    termout off
store  set sqlplus_settings replace
save   buffer.sql replace

-- set parameters for spooling
set pagesize 0
set trimspool on
set linesize 1000
set termout off
set feedback off
set verify off

spool awr_report.html
-- input for instance_no, in the form '1,2,3,4'
select output from table(DBMS_WORKLOAD_REPOSITORY.AWR_GLOBAL_REPORT_HTML(&db_id,'1,2,3,4',&snap_start,&snap_end))
/
spool off

-- reset sqlplus settings
get    buffer.sql nolist
@sqlplus_settings
set    termout on
