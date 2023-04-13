-- Regression for UB (stack-use-after-scope) in extactAll()
SELECT
    '{"a":"1","b":"2","c":"","d":"4"}{"a":"1","b":"2","c":"","d":"4"}{"a":"1","b":"2","c":"","d":"4"}{"a":"1","b":"2","c":"","d":"4"}' AS json,
    extract_all(json, '"([^"]*)":') AS keys,
    extract_all(json, ':"\0[^"]*)"') AS values;
