-- SELECT count(DISTINCT decodeXMLComponent(Title) AS decoded) FROM test.hits WHERE Title != decoded;
SELECT count(DISTINCT decode_xml_component(Title) AS decoded) FROM test.hits WHERE Title != decoded;
