DEFINE	_COMMENT_OUT_THIS="--"	

select owner, table_name, column_name, histogram, NUM_BUCKETS
from DBA_TAB_COL_STATISTICS
where
	owner = upper('&&owner')
	and	table_name = upper('&&table_name')
	and	column_name = upper('&&column_name')
/	

accept changeHist	prompt 'Do you want to change the histogram for this column (y/n)?'
--variable chhist varchar2
--exec :chhist := upper(nvl('&&changeHist','N'))

col comment_out noprint	new_value _COMMENT_OUT_THIS

select	case	when upper(nvl('&&changeHist','N')) = 'N' then '/* comment out the next block'
				else	''
		end comment_out
from dual;

begin
	--if (:chhist = 'Y') THEN
	&_COMMENT_OUT_THIS	dbms_stats.gather_table_stats('&&owner', '&&table_name', method_opt=>'FOR COLUMNS SIZE '||nvl('&&num_of_buckets','AUTO')||' '||upper('&&column_name'));
	--end if;
	null;
end;
/	

undef owner
undef table_name
undef column_name
undef num_of_buckets
undef changeHist
undef _COMMENT_OUT_THIS_BLOCK
col comment_out	clear