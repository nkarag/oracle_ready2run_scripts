-- ----------------------------------------------------------------------------------------------
--	st_histogram.sql
--	DESCRIPTION
--		Returns histogram info for a specific column of a table (from DBA_TAB_COL_STATISTICS). Then
--		the script asks the user if he/she wants to modify the "histogram status" for this column.
--		The user can create or delete a histogram by specifying the appropriate number of buckets, as
--		an input to DBMS_STATS.GATHER_TABLE_STATS, using the "SIZE option" of the method_opt input parameter.
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------
select owner, table_name, column_name, histogram, NUM_BUCKETS, num_distinct
from DBA_TAB_COL_STATISTICS
where
	owner = upper('&&owner')
	and	table_name = upper('&&table_name')
	and	column_name = upper('&&column_name')
/	

accept changeHist	prompt 'Do you want to change the histogram for this column (y/n)?'

-- with the following script (if.sql) we achieve branching in this sqlplus script.
@if	"nvl(upper('&changeHist'),'N') = 'Y'" 
	accept num_of_buckets	prompt 'Choose num_of_buckets (1 delete the histogram, 254 create a histogram, <ENTER> for AUTO):'
	exec	dbms_stats.gather_table_stats('&&owner', '&&table_name', method_opt=>'FOR COLUMNS SIZE '||nvl('&&num_of_buckets','AUTO')||' '||upper('&&column_name'));
-- end if */

undef owner
undef table_name
undef column_name
undef num_of_buckets
undef changeHist
