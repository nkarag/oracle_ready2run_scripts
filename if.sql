-- ----------------------------------------------------------------------------------------------
--	if.sql
--	DESCRIPTION
--		Script to achieve conditional execution (i.e., branching) within an sqlplus script. It is meant to be used
--		within another script (where conditional execution is required) and NOT as a stand alone script.
--
--		This nice idea is taken from:	http://orasql.org/2013/04/17/sqlplus-tips-4-branching-execution/
--		Also the idea of branching is mentioned in this thread: https://community.oracle.com/message/4499960#4499960
--
--		The script accepts a single parameter which is the condition to be evaluated. It is called like this:
--		@if	<CONDITION>
--		Some examples:
--		@if	1=1
--		@if	1=0
--		@if	"&answer = 'Y'"
--		@if	"nvl(upper('&answer'),'N') = 'Y'"
--
--		The if.sql script evaluates the input condition and according to the result (true or false) it invokes script null.sql
--		or script comment_on.sql respectively. Script null.sql is an empty file, while script comment_on.sql is just the following text: /*
--		(i.e., the beginning of a comment)
--
--		***NOTE***
--		Whenever you call if.sql in a script you have to remember to include at the end, the closing end of the comment (*/).
--
--		Here is an example of how you can use the if.sql script inside a script that you need conditional execution:
--	
--			accept answer	prompt 'Give me an answer yes or no (y/n)?'
--			@if "'&answer' = 'y'"
--				select 'you said yes' from dual;
--			-- end if */
--			@if "'&answer' = 'n'"
--				select 'you said no' from dual;
--			-- end if */
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------

col do_next new_val do_next noprint;
select
      case
         when &1 then 'null'
         else 'comment_on'
      end as do_next
from dual;
@&do_next

