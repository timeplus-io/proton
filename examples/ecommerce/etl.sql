-- read the topic via an external stream
CREATE EXTERNAL STREAM frontend_events(raw string)
                SETTINGS type='kafka',
                         brokers='redpanda:9092',
                         topic='owlshop-frontend-events';

-- create the other external stream to write data to the other topic
CREATE EXTERNAL STREAM target(
    _tp_time datetime64(3), 
    url string, 
    method string, 
    ip string) 
    SETTINGS type='kafka', 
             brokers='redpanda:9092', 
             topic='masked-fe-event', 
             data_format='JSONEachRow';

-- setup the ETL pipeline via a materialized view
CREATE MATERIALIZED VIEW mv INTO target AS 
    SELECT now64() AS _tp_time, 
           raw:requestedUrl AS url, 
           raw:method AS method, 
           lower(hex(md5(raw:ipAddress))) AS ip 
    FROM frontend_events;