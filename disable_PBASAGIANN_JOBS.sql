select *
from dba_jobs
where
    upper(what) like '%DELAY%'

/*
JOB    LOG_USER    PRIV_USER    SCHEMA_USER    LAST_DATE    LAST_SEC    THIS_DATE    THIS_SEC    NEXT_DATE    NEXT_SEC    TOTAL_TIME    BROKEN    INTERVAL    FAILURES    WHAT    NLS_ENV    MISC_ENV    INSTANCE
343    PBASAGIANN    PBASAGIANN    PBASAGIANN    25-09-2015 14:18:40    14:18:40    null    null    25-09-2015 14:48:40    14:48:40    1244093    N    SYSDATE+30/1440     0    begin
pbasagiann.dwh_delayed_check_tasks;
end;    NLS_LANGUAGE='AMERICAN' NLS_TERRITORY='AMERICA' NLS_CURRENCY='$' NLS_ISO_CURRENCY='AMERICA' NLS_NUMERIC_CHARACTERS='.,' NLS_DATE_FORMAT='DD/MM/YYYY' NLS_DATE_LANGUAGE='AMERICAN' NLS_SORT='BINARY'    0102000200000000    0
284    PBASAGIANN    PBASAGIANN    PBASAGIANN    25-09-2015 14:15:14    14:15:14    null    null    25-09-2015 14:45:14    14:45:14    1837388    N    SYSDATE+30/1440     0    pbasagiann.dwh_delayed_tasks;    NLS_LANGUAGE='AMERICAN' NLS_TERRITORY='AMERICA' NLS_CURRENCY='$' NLS_ISO_CURRENCY='AMERICA' NLS_NUMERIC_CHARACTERS='.,' NLS_DATE_FORMAT='DD/MM/YYYY' NLS_DATE_LANGUAGE='AMERICAN' NLS_SORT='BINARY'    0102000200000000    0
*/

/*
 connect as SYSDBA and run the following.
 
 NOTE: the DBMS_IJOB package is undocumented. If you try the documented DBMS_JOB then ONLY the onwer of the job can break it!!!
 then you get an:ORA-23421: job number is not a job in the job queue
 
 Only the owner of the job can break it!!!! Not even SYS!!!

*/
exec DBMS_IJOB.BROKEN(343, TRUE)    

exec DBMS_IJOB.BROKEN(284, TRUE)