undef task_name
DECLARE
 my_task_name VARCHAR2(30);
  my_sqltext   CLOB;
BEGIN
	my_sqltext := '&sqltxt';
	my_task_name := DBMS_SQLTUNE.CREATE_TUNING_TASK(sql_text => my_sqltext, user_name => upper('&username'), task_name => '&task_name');
	DBMS_SQLTUNE.EXECUTE_TUNING_TASK( task_name => '&task_name' );
END;
/