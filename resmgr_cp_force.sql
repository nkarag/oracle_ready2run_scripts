/*
In case you need to force a plan instead of the current opened automatically by scheduler windows you can use:
alter system set resource_manager_plan='FORCE:mydb_plan' scope=memory sid='*';

Using the prefix FORCE indicates that the current resource plan can be changed only when the DBA changes the value of the RESOURCE_MANAGER_PLAN
initialization parameter. This restriction can be lifted by rerunning the command without preceding the plan with "FORCE:".

Take care when you do such change that the resource_manager_plan is not hardcoded inside the spfile. Check by inspecting:
Sql> create pfile=’/tmp/initDWHPRD.ora’ from spfile;
If the parameter exists with ‘*’ or for any instance you need to reset it from spfile (otherwise you would get this plan if instance is restarted) by command:
Alter system reset resource_manager_plan scope=spfile sid=’*’;

or

Alter system reset resource_manager_plan scope=spfile sid=’DWHPRD<1,2,3,4>’;
In case this is only hardcoded to a specific instance.

*/

alter system set resource_manager_plan='FORCE:&PLAN' scope=memory sid='*';