accept startdate prompt "enter start date ('dd-mm-yyyy'):"
accept enddate prompt "enter end date ('dd-mm-yyyy'):"

declare
	lsday date;
	leday date;
	timespent integer;
begin
	lsday := to_date('&startdate', 'dd-mm-yyyy');
	leday := to_date('&enddate', 'dd-mm-yyyy');
	LOOP
		select abs(mod(dbms_random.random, 4)) into timespent from dual; 
		
		insert into mylife.days(day, yesterday, status, time_spent) 
			values (lsday,lsday-1,default, decode(timespent,0,'FAMILY',1,'FRIENDS', 2, 'ORACLE', 'OTHER'));
		commit;
		lsday := lsday + 1;
		EXIT WHEN lsday = leday;
	END LOOP;
end;
/	