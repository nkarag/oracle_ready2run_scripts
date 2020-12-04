set    termout off     
   
store  set sqlplus_settings replace
save   buffer.sql replace

-- for large sql statements
set    longchunksize 100000
set    linesize 500
set    long 100000
set    heading off verify off autotrace off feedback off
set    timing off
set    wrap on
set    pagesize 1000

--create table MONITOR_DW.sqlhist_index
--as 
--select rownum rn, rowid rid 
--from ( 
--    select *           
--    from monitor_dw.sql_history
--    where
--    PARSING_SCHEMA_NAME in     
--    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
--    and COMMAND_TYPE_DESC = 'SELECT'
--    --and avg_etime > 60*60*(15/60) 
--);

def filename
column rs new_value filename

select DBMS_RANDOM.STRING('u', 10)||'.sql' rs from dual;
 

spool  &filename replace

select * from MONITOR_DW.sqlhist_index sample(50)

select sql_text||';'||chr(10)||' exec  DBMS_LOCK.sleep('||mod(abs(dbms_random.random),11)||')'||chr(10)
from (
    select *           
    from monitor_dw.sql_history
    where
    PARSING_SCHEMA_NAME in     
    ( select SCHEMA_NAME from MONITOR_DW.MONDW_INDEX_USAGE_TRG_SCHEMAS where INCLUDE_IND = 1)
    and COMMAND_TYPE_DESC = 'SELECT'
    --and avg_etime > 60*60*(15/60)
    order by  dbms_random.random
) 
;  
--where
--rowid = (select rid from monitor_dw.sqlhist_index where rn = (select abs(mod(dbms_random.random, 108)) from dual));
--sql_id = 'aw2m7mbjgwk4t';

select '/' from dual;

spool  off

--drop table MONITOR_DW.sqlhist_index;

-- run queries
@@ &filename


get    buffer.sql nolist
@sqlplus_settings
set    termout on   