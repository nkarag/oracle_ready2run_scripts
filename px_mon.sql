/*
	Monitor active sessions running parallel queries: 
	You can monitor the Query Coordinator (QC) session as well as the Parallel slave sessions. You can see stuff as:
	Requestes DOP, Actual DOP, Number of Sesssions, QC session, Elapsed time that QC session has been active, etc.
*/

CLEAR BREAKS
CLEAR COMPUTES

SELECT DECODE (	px.qcinst_id,
				NULL, 
				username,
                ' - '|| LOWER (SUBSTR (pp.SERVER_NAME, LENGTH (pp.SERVER_NAME) - 4, 4))
			) 												"Username",
         DECODE (px.qcinst_id, NULL, 'QC', '(Slave)')       "QC/Slave",
         s.SQL_ID,
         px.req_degree                                        "Requested DOP",
         px.DEGREE                                            "Actual DOP",
         count(*) over (partition by px.qcsid)                "Num Of Sessions",
         count(*) over (partition by username)                "Num Of Tot Sessions Per User",
         round(DECODE (px.server_set, '', s.last_call_et, '')/60, 1) "Elapsed minutes",  -- This is for the coordinator session only
																				 -- If the session STATUS is currently ACTIVE, then the value represents the elapsed time (in seconds) since the session has become active.
																				 -- If the session STATUS is currently INACTIVE, then the value represents the elapsed time (in seconds) since the session has become inactive.		 
         TO_CHAR (px.server_set)                            "SlaveSet",
         TO_CHAR (s.SID)                                    "SID",
         s.SERIAL#                                          "SERIAL#",
         TO_CHAR (px.inst_id)                               "Slave INST",		 
         s.program,
         DECODE (sw.state, 'WAITING', 'WAIT', 'NOT WAIT')   AS STATE,
         CASE sw.state
             WHEN 'WAITING' THEN SUBSTR (sw.event, 1, 30)
             ELSE NULL
         END									            AS wait_event,
         DECODE (px.qcinst_id, NULL, TO_CHAR (s.SID), px.qcsid)"QC SID",
         TO_CHAR (px.qcinst_id)                               "QC INST"
FROM gv$px_session  px,
	 gv$session     s,
	 gv$px_process  pp,
	 gv$session_wait sw
WHERE     px.SID = s.SID(+)
	 AND px.serial# = s.serial#(+)
	 AND px.inst_id = s.inst_id(+)
	 AND px.SID = pp.SID(+)
	 AND px.serial# = pp.serial#(+)
	 AND sw.SID = s.SID
	 AND sw.inst_id = s.inst_id
ORDER BY DECODE (px.QCINST_ID, NULL, px.INST_ID, px.QCINST_ID),
         px.QCSID,
         DECODE (px.SERVER_GROUP, NULL, 0, px.SERVER_GROUP),
         px.SERVER_SET,
         px.INST_ID
/		 
