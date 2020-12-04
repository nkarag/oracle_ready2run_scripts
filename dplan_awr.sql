set lines 500
select * from table(dbms_xplan.display_awr('&sql_id', format =>'advanced'))
/