REM $Header: 215187.1 sqlt_s66109_export_driver.sql 11.4.5.9 2013/07/15 carlos.sierra $
SET TERM ON;
PRO
PRO *******************************************************************
PRO * Enter SQLTXPLAIN valid password to export SQLT repository       *
PRO * Notes:                                                          *
PRO * 1. If you entered an incorrect password you will have to enter  *
PRO *    now both USER and PASSWORD. The latter is case sensitive     *
PRO * 2. User is SQLTXPLAIN and not your application user.            *
PRO *******************************************************************
HOS exp sqltxplain/^^enter_tool_password. parfile=sqlt_s66109_export_parfile2.txt
HOS exp sqltxplain/^^enter_tool_password. parfile=sqlt_s66109_export_parfile.txt
SET TERM OFF;
HOS chmod 777 sqlt_s66109_import.sh
HOS zip -m sqlt_s66109_tc sqlt_s66109_exp.dmp
HOS zip -m sqlt_s66109_tc sqlt_s66109_import.sh
HOS zip -m sqlt_s66109_tcx sqlt_s66109_exp2.dmp
HOS zip -m sqlt_s66109_log sqlt_s66109_exp.log
HOS zip -m sqlt_s66109_log sqlt_s66109_exp2.log
HOS zip -m sqlt_s66109_driver sqlt_s66109_export_driver.sql
HOS zip -m sqlt_s66109_driver sqlt_s66109_export_parfile.txt
HOS zip -m sqlt_s66109_driver sqlt_s66109_export_parfile2.txt
