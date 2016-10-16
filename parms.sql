----------------------------------------------------------------------------------------
--
-- File name:   parms.sql
-- Purpose:     Display parameters and values.
-
-- Author:      Kerry Osborne
--
-- Usage:       This scripts prompts for three values, all of which can be left blank.
--
--              name: the name (or piece of a name) of the parameter(s) you wish to see
--
--              isset: "TRUE" or "T" to see only nondefault parameters 
--
--              show_hidden: "Y" to show hidden parameters as well
--
--	nkarag:
--		in order to be able to run the script as nkarag and not sys
--		I created two views in the sys schema (you must create a view in order to avoid the: ORA-02030: can only select from fixed tables/views,
--	its not possible to grant access directly on a fixed table like the x$ ones)
-- 	create view v_x$ksppi as select * from sys.x$ksppi
--  create view v_x$ksppsv as select * from sys.x$ksppsv
--  and then granted select access on these views to nkarag
--  Finally, I created two synonyms in nkarag to point to these views:
-- 		create synonym x$ksppi for sys.v_x$ksppi
-- 		create synonym x$ksppsv for sys.v_x$ksppsv
--  and I have changed the from clause in the script below accordingly.
---------------------------------------------------------------------------------------
set lines 155
col name for a50
col value for a70
col isdefault for a8
col ismodified for a10
col isset for a10
select name, value, isdefault, ismodified, isset
from
(
select flag,name,value,isdefault,ismodified,
case when isdefault||ismodified = 'TRUEFALSE' then 'FALSE' else 'TRUE' end isset 
from
   (
       select 
            decode(substr(i.ksppinm,1,1),'_',2,1) flag
            , i.ksppinm name
            , sv.ksppstvl value
            , sv.ksppstdf  isdefault
--            , decode(bitand(sv.ksppstvf,7),1,'MODIFIED',4,'SYSTEM_MOD','FALSE') ismodified
            , decode(bitand(sv.ksppstvf,7),1,'TRUE',4,'TRUE','FALSE') ismodified
         from x$ksppi  i -- nkarag.x$ksppi  i -- sys.x$ksppi  i
            , x$ksppsv sv -- nkarag.x$ksppsv sv -- svsys.x$ksppsv sv
        where i.indx = sv.indx
   )
)
where name like nvl('%&parameter%',name)
and upper(isset) like upper(nvl('%&isset%',isset))
and flag not in (decode('&show_hidden','Y',3,2))
order by flag,replace(name,'_','')
/
