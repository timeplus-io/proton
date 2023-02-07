-- Tags: no-fasttest
desc format(JSONEachRow, '{"x" : 1, "y" : "string", "z" : "0.0.0.0" }') settings schema_inference_hints='x uint8, z ipv4';
desc format(JSONEachRow, '{"x" : 1, "y" : "string"}\n{"z" : "0.0.0.0", "y" : "string2"}\n{"x" : 2}') settings schema_inference_hints='x uint8, z ipv4';
desc format(JSONEachRow, '{"x" : null}') settings schema_inference_hints='x nullable(uint32)';
desc format(JSONEachRow, '{"x" : []}') settings schema_inference_hints='x array(uint32)';
desc format(JSONEachRow, '{"x" : {}}') settings schema_inference_hints='x map(string, string)';

desc format(CSV, '1,"string","0.0.0.0"') settings schema_inference_hints='c1 uint8, c3 ipv4';
desc format(CSV, '1,"string","0.0.0.0"') settings schema_inference_hints='x uint8, z ipv4', column_names_for_schema_inference='x, y, z';
desc format(CSV, '\\N') settings schema_inference_hints='x nullable(uint32)', column_names_for_schema_inference='x';
