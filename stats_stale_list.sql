set serveroutput on 
spool stale_stats_segments.spool
DECLARE 
ObjList dbms_stats.ObjectTab; 
BEGIN 
dbms_stats.gather_database_stats(objlist=>ObjList, options=>'LIST STALE'); 
FOR i in ObjList.FIRST..ObjList.LAST 
LOOP 
dbms_output.put_line(ObjList(i).ownname || '.' || ObjList(i).ObjName || ' ' || 
ObjList(i).ObjType || ' ' || ObjList(i).partname); 
END LOOP; 
END; 
/ 
spool off 