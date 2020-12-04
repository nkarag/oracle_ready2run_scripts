accept answer	prompt 'Give me an answer yes or no (y/n)?'
@if "'&answer' = 'y'"
	select 'you have said yes' from dual;
-- end if */
@if "'&answer' = 'n'"
	select 'you have said no' from dual;
-- end if */