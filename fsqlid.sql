-------------------------------------------------------------------------------------
-- fsqlid.sql
--
--	Description
--		Input the sql text and returns the sql_id.
--		the script calls function dbms_sqltune_util0.sqltext_to_sqlid which requires
--		access to the SYS account. However, if you dont have access to the SYS account
--		in the database you work, you can have on another database e.g. the one on 
--		your laptop. Remember that SQL_IDs are the same on all databases irrespective of 
--		database version! So you can get the sql_id from there.
--
--		Notes:
-- 			* 	Notice how the string containing single quotes is itself quoted. This syntax q'[...]', fully documented
--				in the SQL Language Reference manual, is very useful but is often missed by many 
--				experienced Oracle specialists.
--			*	It is necessary to append a NUL character to the end of the text before calling the function.
-------------------------------------------------------------------------------------

select dbms_sqltune_util0.sqltext_to_sqlid( q'[&sql_text]' || CHR(0) )  sql_id from dual
/