prompt Enable trace for a specific session (in the same instance!). Press Enter to enable for the current session
begin
dbms_monitor.session_trace_enable(
	session_id => '&sid',
	serial_num => '&serial'
);
end;
/