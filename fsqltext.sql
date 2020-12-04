@@sqlplus_settings_store

set    longchunksize 1000000
set    linesize 9999
set    long 1000000
set    heading off verify off autotrace off feedback off
set    timing off
set    wrap on
set    pagesize 1000

col sql_fulltext format A64 WORD_WRAPPED 

select sql_fulltext 
from (
select sql_fulltext
from gv$sqlarea
where sql_id = '&sql_id'
)
where rownum = 1
/

/*

col sql_text format A60000 WORD_WRAPPED 

select sql_text 
from (
select sql_text, row_number() over(partition by sql_text order by inst_id) r
from gv$sqltext_with_newlines
where sql_id = '&sql_id'
order by piece
)
where r = 1
/
*/

@@sqlplus_get_settings