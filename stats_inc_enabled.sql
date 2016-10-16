prompt is incremental stats gathering enabled?
select DBMS_STATS.GET_PREFS('INCREMENTAL', upper('&table_owner'), upper('&table_name')) from dual
/
