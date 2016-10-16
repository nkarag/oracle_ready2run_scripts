l_message := l_message || ... -- wrong way to 


declare
    vrData  RAW(32767);
    l_message    clob;
    l_message_varchr    varchar2(4000);  
begin
    
    -- loop until no more characters in clob
        -- substr 4000 characters from clob and cast them to raw
        -- call UTL_smtp.write_raw_data with each chunk of raw data (4000 characters)
    -- end loop         
    null;
end;


   