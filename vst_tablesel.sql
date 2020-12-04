/**************************************************************************
	Get table selectivity. Returns the percent of rows returned from a table 
	when a filter condition is applied.
	
	Note:
		if filter value contains a quote "'" then you must provide two quotes "''". For example:
		USE_CASE.USE_CASE_FLG = ''Assets''

**************************************************************************/

prompt Give table as it appears in FROM CLAUSE:
def tbl = '&1'

prompt Give filters on &tbl as it appears in WHERE clause:
def flt = '&2.'


COL nrf NEW_V num_rows_filtered
select count(*) nrf from &&tbl. where &&flt.;  


COL nrt NEW_V num_rows_total
select count(*) nrt from &&tbl.;

col selectivity for 99D999
select round(100*&&num_rows_filtered. / &&num_rows_total., 2)||'%' selectivity from dual;

undef tbl
undef flt
undef num_rows_filtered
undef num_rows_total