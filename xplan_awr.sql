set linesize 999
set pagesize 999
--set serveroutput off

REM Note
REM -----
REM - Warning: basic plan statistics not available. These are only collected when:
REM        * hint 'gather_plan_statistics' is used for the statement or
REM        * parameter 'statistics_level' is set to 'ALL', at session or system level

select * from table( dbms_xplan.display_awr('&sql_id', plan_hash_value => '&plan_hash_value', format =>'ALL ALLSTATS LAST -PROJECTION'));

--format =>'ALL ALLSTATS LAST'))


--'ADVANCED +PEEKED_BINDS +ALLSTATS LAST +MEMSTATS LAST') );

