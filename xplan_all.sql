set linesize 999
set pagesize 999
--set serveroutput off

REM Note
REM -----
REM - Warning: basic plan statistics not available. These are only collected when:
REM        * hint 'gather_plan_statistics' is used for the statement or
REM        * parameter 'statistics_level' is set to 'ALL', at session or system level

select * from table( dbms_xplan.display_cursor('&sql_id', '&child_number', 'ADVANCED ALLSTATS LAST'));

-- 'ADVANCED ALLSTATS LAST +ADAPTIVE'));  -- for 12c
--'ADVANCED +PEEKED_BINDS +ALLSTATS LAST +MEMSTATS LAST partition cost') );


--  'ALLSTATS LAST') );
--'ALL LAST'));

