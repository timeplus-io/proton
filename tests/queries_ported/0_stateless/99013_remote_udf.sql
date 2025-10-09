CREATE REMOTE FUNCTION remote_func_1(ip string) RETURNS string 
URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'
AUTH_METHOD 'none';


CREATE Remote FUNCTION remote_func_2(value float32) RETURNS float32 
URL 'https://127.0.0.1/items/'
AUTH_METHOD 'auth_header'
AUTH_HEADER 'auth'
AUTH_KEY 'proton';

show create function remote_func_1;
show create function remote_func_2;
show functions where language = 'Remote';

drop function remote_func_1;
drop function remote_func_2;



