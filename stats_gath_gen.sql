rem -------------------------------------------------------------------------------------------------------------------------
rem 	stats_gath_gen.sql	Spools into file "gather_stats.spool", gather table stats commands for all tables with stale stats.
rem -------------------------------------------------------------------------------------------------------------------------
rem set serveroutput on 
DECLARE 
ObjList dbms_stats.ObjectTab; 
l_cmd	varchar2(4000);
BEGIN
execute immediate 'drop table nkarag.stale_stats';
execute immediate 'create table nkarag.stale_stats(gendate date, cmd varchar2(4000))'; 
dbms_stats.gather_database_stats(objlist=>ObjList, options=>'LIST STALE'); 
FOR i in ObjList.FIRST..ObjList.LAST 
LOOP 
	if (ObjList(i).ObjType = 'TABLE') then
		l_cmd := 'exec dbms_stats.gather_table_stats(ownname=>'''||ObjList(i).ownname || ''', tabname=>''' || ObjList(i).ObjName || ''')';
		insert into nkarag.stale_stats values(sysdate, l_cmd);
    end if;
--dbms_output.put_line('exec dbms_stats.gather_table_stats(ownname=>'''||ObjList(i).ownname || ''', tabname=>''' || ObjList(i).ObjName || ''')'); 
END LOOP; 
commit;
END; 
/ 


accept degree prompt "Enter value for degree:"
set heading off
set feedback off
set verify off
set termout off
set linesize 9999
spool gather_stats.spool

select distinct replace(cmd, ')', ',degree => &degree )') from nkarag.stale_stats order by 1
/

spool off 
set heading on
set feedback on
set verify on
set termout on
