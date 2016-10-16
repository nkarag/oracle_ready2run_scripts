set    termout off
store  set sqlplus_settings replace
save   buffer.sql replace

REM set colsep ';'
rem the following supresses all headers
set pagesize 0
set trimspool on
set linesize 1000
set termout off
set feedback off

spool myfile.csv replace

-- query
select owner||','||table_name||','||tablespace_name
from all_tables
where owner = 'SYS'
and tablespace_name is not null
/
spool off

get    buffer.sql nolist
@sqlplus_settings
set    termout on

REM
REM in order to clean out the extra whitespace in each column that sqlplus generates
REM use a tool like sed, eg. sed -r 's/(\S+)\s+;/\1;/' myfile.csv > myfile2.csv
REM