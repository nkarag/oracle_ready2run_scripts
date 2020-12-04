DROP TABLE x_v_assoc;

CREATE TABLE x_v_assoc (
	x_id number,
	v_id number);

 
set serveroutput on size 1000000
declare
	CURSOR v_views IS SELECT name, object_id FROM v$fixed_table WHERE name like 'V$%' ORDER BY name;
	CURSOR plan_curs(p_hash_value number) IS SELECT DISTINCT nvl(object#, -1) object#, object_name
	FROM v$sql_plan 
	WHERE hash_value=p_hash_value 
	AND operation LIKE 'FIXED TABLE%';
	prev_hash_value number;
	object_name varchar2(30);
	v_cursor integer;
	result integer;
	object_id number;
begin
	FOR v_rec IN v_views LOOP
		BEGIN
			dbms_output.put_line(v_rec.name);
			v_cursor := dbms_sql.open_cursor;
			-- parse and execute a statement that selects from the v$ view but returns no rows
			dbms_sql.parse(v_cursor, 'SELECT * FROM '||v_rec.name||' WHERE rownum=0', dbms_sql.native);
			result:=dbms_sql.execute_and_fetch(v_cursor);
			-- get hash value of previous statement, i.e. select from v$view
			SELECT prev_hash_value INTO prev_hash_value 
			FROM gv$session 
			WHERE audsid=userenv('sessionid')
			AND rownum=1; -- just in case several child cursors exist, prev_child_number not available in 9i
			dbms_output.put_line('prev_hash_value: '||prev_hash_value);
			dbms_sql.close_cursor(v_cursor);
			FOR plan_rec IN plan_curs(prev_hash_value) LOOP
				-- object_name may have this format: X$KEWMEVMV (ind:1)
				-- object_id is NULL for fixed tables
				result:=instr(plan_rec.object_name, ' ');
				IF result > 0 THEN
					object_name:=substr(plan_rec.object_name,1,result-1);
				ELSE
					object_name:=plan_rec.object_name;
				END IF;
				SELECT object_id INTO object_id FROM v$fixed_table WHERE name=object_name;
				IF SQL%NOTFOUND THEN
					dbms_output.put_line('object_id for '||object_name||' not found in V$FIXED_TABLE');
				ELSE
					dbms_output.put_line('	'||object_name||': '||object_id);
				END IF;
				INSERT INTO x_v_assoc(x_id, v_id) VALUES(object_id, v_rec.object_id);
			END LOOP;
		EXCEPTION WHEN OTHERS THEN 
			dbms_output.put_line(dbms_utility.format_error_stack);
		END;
	END LOOP;
end;
/
commit;

spool x_views
set lines 140
set trimout on
set trimspool on
break on x_name noduplicates
-- x$ tables used by v$ views
SELECT 
f1.name x_name, 
f2.name v_name
FROM x_v_assoc a, v$fixed_table f1, v$fixed_table f2
WHERE a.x_id=f1.object_id
AND a.v_id=f2.object_id
ORDER BY x_name;
clear breaks

break on v_name noduplicates
-- v$views and underlying x$ tables
SELECT 
f1.name v_name, 
f2.name x_name
FROM x_v_assoc a, v$fixed_table f1, v$fixed_table f2
WHERE a.v_id=f1.object_id
AND a.x_id=f2.object_id
ORDER BY v_name;

spool off

