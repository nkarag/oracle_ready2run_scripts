@@sqlplus_settings_store

set    longchunksize 1000000
set    linesize 32767
set    long 1000000
set    heading off verify off autotrace off feedback off
set    timing off
set    wrap on
set    pagesize 1000

col sql_text format A64 WORD_WRAPPED 

select SQL_TEXT
from DBA_HIST_SQLTEXT
where sql_id = '&sql_id'
/

@@sqlplus_get_settings