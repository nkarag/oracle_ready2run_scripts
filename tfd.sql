prompt Disable trace for a specific session (in the same instance!). Press Enter to disable for the current session
begin
	dbms_monitor.session_trace_disable (
	session_id => '&sid',
	serial_num => '&serial'
	);
end;
/	