-- ----------------------------------------------------------------------------------------------
--	sqltune_exec.sql
--  	Tune a specific sql_id by creating a tuning task and calling DBMS_SQLTUNE.EXECUTE_TUNING_TASK
--		(Note: you must login to the same instance as the one running the sql_id because the script assumes the sql_id is loaded in the library cache)
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------


DECLARE
 my_task_name VARCHAR2(30);
BEGIN
 my_task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(sql_id => trim('&sqlid'), task_name => '&&task_name', time_limit => nvl('&time_limit_in_secs', DBMS_SQLTUNE.TIME_LIMIT_DEFAULT) );
 DBMS_SQLTUNE.EXECUTE_TUNING_TASK( task_name => '&task_name' );
END;
/

undef task_name