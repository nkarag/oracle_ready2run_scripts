-- ----------------------------------------------------------------------------------------------
--	sqltune_report.sql
--  	Report of the results of a sql tuning task (including recommendations with respective sql statements)
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

@sqlplus_settings_store

SET LONG 1000000
SET LONGCHUNKSIZE 1000000
SET LINESIZE 9999
set pagesize 10000
set    heading off verify off autotrace off feedback off
set    wrap on



SELECT DBMS_SQLTUNE.REPORT_TUNING_TASK( trim('&task_name'), 'TEXT', 'ALL', 'ALL', owner_name=>upper('&owner_name') )
FROM   DUAL
/

undef task_name

@sqlplus_get_settings