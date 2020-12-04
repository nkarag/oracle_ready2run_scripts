----------------------------------------------------------------------------------------
--
-- File name:   spool2csv.sql
--
-- Purpose:     Generates an sqlplus script and then runs it that spools a specific table to a CSV file.
--
--
-- Version:     2013/10/19
--
-- Usage:       This script inputs 4 parameters. 
--				Parameter 1 is the table owner.
-- 				Parameter 2 is the table name of the table that we want to spool.
--				Parameter 3 is the name of the CSV file name
--				Parameter 4 is the column separator that we want to use.
--
-- Example:     @spool2csv.sql	
--
-- Notes: 	    
--				*Developed and tested on 11.2.0.1
--
--				*The generated script is called "do_spoolcsv.sql" and it can be run seperately (you can 
--				  comment out the line where this script is called). However, if you want to run the generated do_spoolcsv.sql as a 
--				  stand alone script, then you have to add at the beginning the sqlplus settings for the right spooling. Just copy the
--				  setting you see in the section "-- set parameters for spooling" below.
--
--				*Note: For tables which contain columns with greek characters the csv file does not depict them correctly.
--				  In this case you have to set the OS enviroinment variable NLS_LANG correctly before running sqlplus 
--				  and calling this script. For example on Linux bash shell you must set: export NLS_LANG=GREEK_GREECE.EL8ISO8859P7
--
--				*If you have a query from which you want to create a CSV file and not a table, then create a view with this query 
--				  and use this script.
--             
-- Author:      Nikos Karagiannidis (http://oradwstories.blogspot.gr/)
---------------------------------------------------------------------------------------

-- save sqlplus settings 
@sqlplus_settings_store

--set    termout off
--store  set sqlplus_settings replace
--save   buffer.sql replace


-- get input table owner
--accept table_owner prompt 'enter value for table owner:'
prompt 'enter value for table owner:'
def table_owner  = '&1'


-- get input table name
--accept table_name prompt 'enter value for table name:'
prompt 'enter value for table name:'
def table_name = '&2'

-- get input csv file name
--accept csvfname prompt 'enter value for csv file name:'
prompt 'enter value for csv file name:'
def csvfname = '&3'

-- get input column separator
--accept column_separator prompt 'enter value for column separator:'
prompt 'enter value for column separator:'
def column_separator = '&4'

-- set parameters for spooling
set pagesize 0
set trimspool on
set linesize 1000
set termout off
set feedback off
set verify off
set timing off

-- get number of columns of table
column maxc new_value max_columns
select max(column_id) maxc from all_tab_columns where owner = upper('&table_owner') and table_name = upper('&table_name');

-- spool on
spool do_spoolcsv.sql replace

-- write save sqlplus settings
--prompt termout off
--prompt store  set sqlplus_settings replace
--prompt save   buffer.sql replace

-- write sqlplus parameters to script file
-- suppress sql output in results 
--prompt set echo off 
-- eliminate row count message at end 
--prompt set feedback off  
-- make line long enough to hold all row data 
--prompt set linesize 1000 
-- suppress headings and page breaks 
--prompt set pagesize 0 
-- eliminate SQL*Plus prompts from output 
--prompt set sqlprompt ''  
-- eliminate trailing blanks 
--prompt set trimspool on  

-- set nls_date_format so to print dates correclty
prompt alter session set nls_date_format='dd-mm-yyyy hh24:mi:ss'
prompt /

-- send output to file 
prompt
prompt spool &&csvfname replace
prompt
-- write query to script file based on the structure of the table as it appears in all_tab_columns
select 'select' from dual;
select  case when column_id < &max_columns then
			''''||column_name||''''||'||'||'''&column_separator'''||'||'
		else
			''''||column_name||''''
	    end
from all_tab_columns
where
	owner = upper('&&table_owner') and table_name = upper('&&table_name')
order by column_id;
select 'from dual' from dual;
select 'union all' from dual;
select 'select' from dual;
select  case when column_id < &&max_columns then
			column_name||'||'||'''&&column_separator'''||'||'
		else
			column_name
		end
from all_tab_columns
where
	owner = upper('&&table_owner') and table_name = upper('&&table_name')
order by column_id;	
select 'from '||'&&table_owner'||'.'||'&&table_name'||';' from dual;

--------------------------------	
-- example query
--
-- select 'EMPLOYEE_ID','LAST_NAME','FIRST_NAME','SALARY' from dual 
-- union all 
-- select employee_id || ',' || 
--        last_name || ',' || 
--        first_name || ',' || 
--        salary 
-- from employees;
-------------------------------- 

-- write spool off
prompt
prompt spool off
prompt
-- write reset sqlplus settings
--prompt get    buffer.sql nolist
--prompt @sqlplus_settings
--prompt set    termout on
-- spool off
spool off

-- run script
@@do_spoolcsv.sql

undef 1
undef 2
undef 3
undef 4
undef table_owner
undef table_name
undef csvfname
undef column_separator
undef max_columns

-- reset sqlplus settings
--get    buffer.sql nolist
--@sqlplus_settings
--set    termout on
@sqlplus_get_settings 