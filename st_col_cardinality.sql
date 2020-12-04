-- ----------------------------------------------------------------------------------------------
--	st_col_cardinality.sql
--
--	DESCRIPTION
--		Returns the cardinality (i.e., number of rows) for a specific value of a specific column of a table.
--		Essentially, it returns "what the optimizer thinks" (based on statistics - histograms or not) that the
--		frequency for the specific value is. It is used in order to check the correctness of the optimizer frequency 
--		calculation for a specific value.
--
--		In order to achieve this, we invoke "explain plan" with a simple query with an equality predicate on the correpsonding column
--		for this value. Then, we get the cardinality of the top level operation in the plan (id = 0)
--
--		Note: this script only supports the NUMBER, VARCHAR2 and DATE data types for columns
--		
--		Examples:
--			For a NUMBER data type column:
--				Enter value for owner: TARGET_DW
--				Enter value for table_name: CUSTOMER_DIM
--				Enter value for column_name: CUSTOMER_SK
--				Enter value for column_value: 230000
--
--			For a VARCHAR2 data type column:
--				Enter value for owner: TARGET_DW
--				Enter value for table_name: CUSTOMER_DIM
--				Enter value for column_name: TRN_NUM
--				Enter value for column_value: 071965080
--
--			For a DATE data type column:
--				Enter value for owner: TARGET_DW
--				Enter value for table_name: CUSTOMER_DIM
--				Enter value for column_name: OTE_BASIC_SEG_FROM_DATE
--				Enter value for column_value: 12/12/2012
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

-- get the column data type
column data_type new_value col_data_type

select  data_type 
from dba_tab_columns
WHERE        owner = upper('&&owner')
	and    table_name = upper('&&table_name')
	and    column_name = upper('&&column_name')
/	

-- build the correct literal value according to the column data type
column literal new_value literal_value
column stmnt_id new_value statement_id

select	case	when '&&col_data_type' = 'NUMBER' THEN '&&column_value'
				when '&&col_data_type' = 'DATE' THEN  'to_date('''||'&&column_value'||''', ''dd/mm/yyyy'')'
				when '&&col_data_type' = 'VARCHAR2' THEN ''''||&&column_value||''''
		end literal,
		''''||'&&column_value'||'''' stmnt_id	
from dual;

EXPLAIN PLAN SET STATEMENT_ID &&statement_id FOR SELECT * FROM &&owner..&&table_name WHERE &&column_name = &&literal_value;

SELECT statement_id "Value", cardinality
FROM plan_table
WHERE id = 0 AND statement_id = &&statement_id;

undef owner
undef table_name
undef column_name
undef column_value
undef col_data_type
undef literal_value
undef statement_id