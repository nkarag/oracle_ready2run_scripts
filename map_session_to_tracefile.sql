SET ECHO OFF
REM ***************************************************************************
REM ******************* Troubleshooting Oracle Performance ********************
REM ************************* http://top.antognini.ch *************************
REM ***************************************************************************
REM
REM File name...: map_session_to_tracefile.sql
REM Author......: Christian Antognini
REM Date........: August 2008
REM Description.: You can use this script to map a session ID to a trace file.
REM Notes.......: Up to 10.1 the value of the initialization parameter
REM               tracefile_identifier (v$process.traceid) is visible only to
REM               the session that set it.
REM Parameters..: -
REM
REM You can send feedbacks or questions about this script to top@antognini.ch.
REM
REM Changes:
REM DD.MM.YYYY Description
REM ---------------------------------------------------------------------------
REM
REM ***************************************************************************

SET TERMOUT ON
SET FEEDBACK OFF
SET VERIFY OFF
SET SCAN ON

@../connect.sql

SET ECHO ON

SELECT s.sid,
       s.server,
       lower(
         CASE  
           WHEN s.server IN ('DEDICATED','SHARED') THEN 
             i.instance_name || '_' || 
             nvl(pp.server_name, nvl(ss.name, 'ora')) || '_' || 
             p.spid
           ELSE
             NULL
         END 
       ) ||
       CASE
         WHEN p.traceid IS NOT NULL THEN
           '_' || p.traceid
         ELSE
           ''
       END ||
       '.trc' AS trace_file_name
FROM v$instance i, 
     v$session s, 
     v$process p, 
     v$px_process pp, 
     v$shared_server ss
WHERE s.paddr = p.addr
AND s.sid = pp.sid (+)
AND s.paddr = ss.paddr(+)
AND s.type = 'USER'
ORDER BY s.sid;
