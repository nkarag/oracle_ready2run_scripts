select *
from DBA_TAB_COL_STATISTICS
where
	owner = upper('&&owner')
	and	table_name = upper('&&table_name')
	and	column_name = upper('&&column_name')
/	

accept changeHist	prompt 'Do you want to change the histogram for this column (y/n)?'
variable chhist varchar2
exec :chhist := upper(nvl('&&changeHist','N'))
 
begin
	if (:chhist = 'Y') THEN
		dbms_stats.gather_table_stats('&&owner', '&&table_name', method_opt=>'FOR COLUMNS SIZE '||nvl('&&num_of_buckets','AUTO')||' '||upper('&&column_name'));
	end if;
end;
/	

undef owner
undef table_name
undef column_name
undef num_of_buckets
undef changeHist