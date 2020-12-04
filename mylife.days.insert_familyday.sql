prompt 'dd-mm-yyyy' 
insert into days(day, yesterday, status, time_spent) values (to_date('&&date', 'dd-mm-yyyy'),to_date('&&date', 'dd-mm-yyyy')-1,default, 'FAMILY')
/
commit;
undef date