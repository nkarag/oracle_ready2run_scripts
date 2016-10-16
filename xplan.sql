set linesize 9999
set pagesize 999
--set serveroutput off

select * from table( dbms_xplan.display_cursor('&sql_id', '&child_number', 'ALL ALLSTATS LAST -PROJECTION'));

--'ALLSTATS LAST alias partition cost'));

--'ALLSTATS LAST alias'));

--'ALL ALLSTATS LAST'));

--'TYPICAL LAST'));

--'ADVANCED +PEEKED_BINDS +ALLSTATS LAST +MEMSTATS LAST') );