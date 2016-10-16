-- ----------------------------------------------------------------------------------------------
--	st_histogram_det.sql
--
--	DESCRIPTION
--		Returns histogram details for a specific column of a table (from DBA_TAB_HISTOGRAMS). 
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

col ENDPOINT_ACTUAL_VALUE for a50
col frequency for 99999999999999

set verify off
set timing off

col histogram new_value hist

select owner, table_name, column_name, histogram, NUM_BUCKETS, num_distinct
from DBA_TAB_COL_STATISTICS
where
	owner = upper('&&owner')
	and	table_name = upper('&&table_name')
	and	column_name = upper('&&column_name')
/	

set verify off
set timing off

-- with the following script (if.sql) we achieve branching in this sqlplus script.
@if	"'&&hist' = 'FREQUENCY'" 
	SELECT	ENDPOINT_ACTUAL_VALUE, -- Actual (not normalized) string value of the endpoint for this bucket
			endpoint_value "endpoint_value - bucket number",  -- the Histogram bucket number
			endpoint_number as "endpoint_number - cumltd freq", -- the cumulated frequency
			endpoint_number - lag(endpoint_number,1,0) OVER (ORDER BY endpoint_number) AS frequency        
	FROM dba_tab_histograms
	WHERE        owner = upper('&&owner')
		and    table_name = upper('&&table_name')
		and    column_name = upper('&&column_name')
	ORDER BY endpoint_number
	/
-- end if */

@if	"'&&hist' = 'HEIGHT BALANCED'" 
	SELECT  ENDPOINT_ACTUAL_VALUE, -- Actual (not normalized) string value of the endpoint for this bucket
			endpoint_value "endpoint_value - in num format",  -- the Histogram bucket number
			endpoint_number as "endpoint_number - bucket num", -- the cumulated frequency
			'N/A' AS frequency        
	FROM dba_tab_histograms t
	WHERE        owner = upper('&&owner')
		and    table_name = upper('&&table_name')
		and    column_name = upper('&&column_name')       
	ORDER BY endpoint_number
	/
-- end if */

@if	"'&&hist' = 'NONE'" 
	prompt There is no histogram for this column. No details are available.
-- end if */

undef owner
undef table_name
undef column_name
undef hist

set verify on
set timing on