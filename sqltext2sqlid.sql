------------------------------------------------------------------------------
-- sqltext2sql_id.sql
--	
--	Description
--		Computes the sql_id given an sql text. It is based on a custom function created by
--		Carlos Sierras. You must create the function first.
-------------------------------------------------------------------------------

select compute_sql_id('&sqltext') SQL_ID from dual;