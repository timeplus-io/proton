select get_server_port('tcp.port');

select get_server_port('unknown'); -- { serverError 170 }
