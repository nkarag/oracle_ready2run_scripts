@sqlplus_settings_store

set    longchunksize 1000000
set    linesize 999
set    long 1000000
set    heading off verify off autotrace off feedback off
set    timing off
set    wrap on
set    pagesize 1000

rem set long 999999999
rem set lines 9999
--col report for a379
--accept sid  prompt "Enter value for sid: "
accept sql_id  prompt "Enter value for sql_id: "
--accept sql_exec_id  prompt "Enter value for sql_exec_id: " default 16777216

set termout off


spool sm.html replace

SELECT 
        DBMS_SQLTUNE.REPORT_SQL_MONITOR( 
		  sql_id => '&&sql_id',
          report_level=>'ALL', 
          type => 'ACTIVE') as report 
FROM dual;

spool off

rem set lines 155
rem undef SID
undef sql_id
rem undef sql_exec_id
 

 
host chrome.exe  file:///F:/office_backup/backup/myscripts/ready2run/sm.html

rem	the following works from cygwin command line but
rem	does not work from within sqlplus, with host command
rem host export p=`pwd`
rem host export myhtml=`cygpath -w ${p}`
rem host  chrome "file:///"${myhtml}"/sm.html" &


set termout on
set    heading on verify on feedback on

@sqlplus_get_settings