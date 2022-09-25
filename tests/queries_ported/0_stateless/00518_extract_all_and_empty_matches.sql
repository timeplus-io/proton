SELECT
    '{"a":"1","b":"2","c":"","d":"4"}' AS json,
    extract_all(json, '"([^"]*)":') AS keys,
    extract_all(json, ':"([^"]*)"') AS values;
