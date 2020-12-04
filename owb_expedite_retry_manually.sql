/*
    Runs  Oracle workflow expedite retry manually
*/
declare 
   v_activity_label varchar2(500); 
BEGIN
    v_activity_label := owf_mgr.Wf_Engine.GetActivityLabel
    ( 
                actid => &activity_id
    );
            
    owf_mgr.Wf_Engine.HandleError
    (
        itemtype => '&item_type',
        itemkey  => '&item_key',
        activity => v_activity_label,
        command  => 'RETRY',
        result   => ''
    );
    Commit;
END;
/
