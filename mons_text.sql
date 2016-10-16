@sqlplus_settings_store

set    longchunksize 1000000
set    linesize 999
set    long 1000000
set    heading off verify off autotrace off feedback off
set    timing off
set    wrap on
set    pagesize 1000

accept sql_id  prompt "Enter value for sql_id: "

/*
var	g_report	CLOB

begin
	:g_report := DBMS_SQLTUNE.REPORT_SQL_MONITOR( 
		  sql_id => '&&sql_id'); --,
          --report_level=>'ALL');--, 
          ---type => 'TEXT');
end;
/
print :g_report
*/
set termout off
spool sm.txt replace

SELECT DBMS_SQLTUNE.report_sql_monitor(
  sql_id       => '&&sql_id',
  type         => 'TEXT',
  report_level => 'ALL') AS report 
FROM dual;

spool off

host cat sm.txt

undef sql_id

@sqlplus_get_settings