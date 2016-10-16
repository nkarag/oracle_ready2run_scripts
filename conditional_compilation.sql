-- A simple Example for PL/SQL conditional compilation within an SQL*Plus script

-- supress variable substitution messages
set verify off

-- step 1: Set dynamic condition, e.g., accept some user input and form the condition to be evaluated
accept answer	prompt 'Give me an answer yes or no (y/n)?'

-- step 2: Evaluate condition, i.e.,"IF-THEN-ELSE logic"
col condition_result1  new_value _COMMENT_OUT_1st_PART
col condition_result2  new_value _COMMENT_OUT_2nd_PART
select
      case
         when '&&answer' = 'y' then '' 
         else '--'
      end as condition_result1,
      case
         when '&&answer' = 'n' then '' 
         else '--'
      end as condition_result2	  
from dual;

-- step 3: Comment-out parts of code based on step 2
--&&_COMMENT_OUT_THIS_PART  select 'you have said ''yes''!' from dual;
begin
	&&_COMMENT_OUT_1st_PART  dbms_output.put_line('you have said ''yes''!');
	&&_COMMENT_OUT_2nd_PART  dbms_output.put_line('you have said ''no''!');
end;
/	
undef answer
undef _COMMENT_OUT_1st_PART
undef _COMMENT_OUT_2nd_PART