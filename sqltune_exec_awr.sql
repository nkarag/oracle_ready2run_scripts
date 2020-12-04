undef task_name
DECLARE
 my_task_name VARCHAR2(30);
BEGIN
 my_task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(begin_snap=>&begin_snap, end_snap=>&endsnap,sql_id => trim('&sqlid'), task_name => '&&task_name', time_limit => nvl('&time_limit_in_secs', DBMS_SQLTUNE.TIME_LIMIT_DEFAULT));
 DBMS_SQLTUNE.EXECUTE_TUNING_TASK( task_name => '&task_name' );
END;
/