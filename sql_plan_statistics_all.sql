COLUMN id FORMAT 99
COLUMN operation FORMAT a18
COLUMN options FORMAT a11
COLUMN actual_time FORMAT 99.999 HEADING "Actual|Time"
COLUMN object_name FORMAT a17 HEADING "Object|Name"
COLUMN last_starts FORMAT 9999999 HEADING "Last|Starts"
COLUMN actual_rows FORMAT 9999999 HEADING "Actual|Rows"
 
SELECT 	id
        ,LPAD (' ', DEPTH) || operation operation
        ,options
        ,last_elapsed_time / 1e6 actual_time
        ,object_name
        ,last_starts
        ,last_output_rows a_rows
		, last_starts * cardinality e_rows_x_starts
		, LAST_CR_BUFFER_GETS bgets
		, LAST_DISK_READS     pread
		, LAST_DISK_WRITES    pwrites
		, LAST_MEMORY_USED
		, LAST_TEMPSEG_SIZE
		, LAST_EXECUTION		
FROM gv$sql_plan_statistics_all
WHERE sql_id = '&sql_id'
ORDER BY child_number,id
/