SELECT extract_url_parameter('?_', '\0_________________________________');
SELECT extract_url_parameter('?abc=def', 'abc\0def');
SELECT extract_url_parameter('?abc\0def=Hello', 'abc\0def');
SELECT extract_url_parameter('?_', '\0');
SELECT extract_url_parameter('ZiqSZeh?', '\0');
SELECT 'Xx|sfF', match('', '\0'), [], ( SELECT cut_url_parameter('C,Ai?X', '\0') ), '\0';
