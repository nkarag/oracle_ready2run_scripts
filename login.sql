set termout off

--define _editor=vim-nox
define _editor=vim

--set serveroutput on size 1000000 format wrapped
set serveroutput off

column object_name format a30
column segment_name format a30
column file_name format a40
column nam format a30
column file_name format a30
column what format a30 word_wrapped
column plan_plus_exp format a100

set trimspool on

set long 5000

--set linesize 180
set linesize 9999

set pagesize 9999

set truncate on

define gname=idle
column global_name new_value gname
select lower(user) || '@' || substr(global_name, 1, decode(dot, 0, length(global_name), dot-1)) global_name
from (select global_name, instr(global_name,'.') dot from global_name );
set sqlprompt '&gname> '

set termout on

alter session set NLS_DATE_FORMAT='dd/mm/yyyy hh24:mi:ss';

set timing on 

