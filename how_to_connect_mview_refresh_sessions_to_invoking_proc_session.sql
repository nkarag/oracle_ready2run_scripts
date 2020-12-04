/*
how to "connect" the sessions generated from a call
to DBMS_SNAPSHOT.REFRESH to the session that represents the OWB node (procedure)
that invokes the refresh.
*/

-- Example of a procedure that refreshes a list of MVIEWS in PER_MAIN: REFRESH_KPIDW_MVGLOB1_PROC
--      PERIF.ETL_PRES_TRANSFORMATION_PKG.REFRESH_KPIDW_MVGLOB1_PROC

-- 1.
-- Lets see if dba_dependencies records the dependency of the procedure to specific MVIEWS
select *
from dba_dependencies t
where 1=1
    AND T.REFERENCED_NAME = 'WOMS_BB_ACT_DEMANDS_VDSL_MV' --'ETL_PRES_TRANSFORMATION_PKG' --'REFRESH_KPIDW_MVGLOB1_PROC'
    AND T.REFERENCED_OWNER = 'KPI_DW'

--    AND T.NAME = 'WOMS_BB_ACT_DEMANDS_VDSL_MV' --'ETL_PRES_TRANSFORMATION_PKG' --'REFRESH_KPIDW_MVGLOB1_PROC'
--    AND T.OWNER = 'KPI_DW'

    --AND T.REFERENCED_NAME = 'ETL_PRES_TRANSFORMATION_PKG' --'REFRESH_KPIDW_MVGLOB1_PROC'
    --AND T.REFERENCED_OWNER = 'PERIF'

--    AND T.NAME = 'ETL_PRES_TRANSFORMATION_PKG' --'REFRESH_KPIDW_MVGLOB1_PROC'
--    AND T.OWNER = 'PERIF'

--  Nope. there is no recorder dependency because the procedure PERIF.ETL_PRES_TRANSFORMATION_PKG.REFRESH_KPIDW_MVGLOB1_PROC
--  uses dynamic SQL to create the list of MVIEWS to be refreshed.

-- 2.
--  lets find all procedures invoking DBMS_MVIEW.REFRESH

with pkgs
as(
select distinct owner, name, type
from dba_source
where
    upper(text) like '%DBMS_MVIEW.REFRESH%'
order by 1,2,3
)
select *
from dba_procedures t1 join pkgs t2 on (t1.owner = t2.owner and t1.object_name = t2.name)
where
    procedure_name like '%REFRESH%MV%' 



select *
from dba_procedures t1 
where
    procedure_name like '%REFRESH%MV%'    

--------------------- DRAFT --------------------
select *
from dba_procedures
where
    procedure_name = 'REFRESH'
    
    
    
select *
from dba_procedures
where
    object_name = 'DBMS_SNAPSHOT'    