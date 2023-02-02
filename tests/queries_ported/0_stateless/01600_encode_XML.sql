SELECT encode_xml_component('Hello, "world"!');
SELECT encode_xml_component('<123>');
SELECT encode_xml_component('&clickhouse');
SELECT encode_xml_component('\'foo\'');