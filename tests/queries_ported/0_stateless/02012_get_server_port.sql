select get_server_port('tcp_port');

select get_server_port('unknown'); -- { serverError 170 }
