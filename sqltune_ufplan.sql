-- ----------------------------------------------------------------------------------------------
--	Unforce a specific plan for a specific sql_id. Input the SQL_HANDLE and PLAN_NAME from dba_sql_plan_baselines. 
--	It drops the corresponding plan from the SQL Plan Baseline (thus works for 11g and above only).--
--
-- PARAMETERS
--   1. SQL_HANDLE (required)	(from dba_sql_plan_baselines)
--   2. PLAN_NAME (required)	(from dba_sql_plan_baselines)
--
-- (C) 2015 Nikos Karagiannidis - http://oradwstories.blogspot.com    
-- ----------------------------------------------------------------------------------------------
declare
	no_plans PLS_INTEGER := 0;
begin
	no_plans := DBMS_SPM.DROP_SQL_PLAN_BASELINE (sql_handle => '&sql_handle', plan_name => '&plan_name');
	
	if (no_plans <> 1) then
		RAISE_APPLICATION_ERROR(-20102, '***Error***: '||no_plans||' number of plans have been dropped for sql_handle = '||:sql_handle||' and plan_name = '||:plan_name);	
	end if;		
end;
/