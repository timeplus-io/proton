select 'a' as a, 'b' as b, 1 as c, 1.1 as d format OpenSearch settings output_format_opensearch_id_column='a';
select 'a' as a, 'b' as b, 1 as c, 1.1 as d format OpenSearch settings output_format_opensearch_id_column='a', output_format_opensearch_include_id_column_in_document=true;

select 'a' as a, 'b' as b, 1 as c, 1.1 as d format OpenSearch settings output_format_opensearch_id_column='a', output_format_opensearch_index_column='b';
select 'a' as a, 'b' as b, 1 as c, 1.1 as d format OpenSearch settings output_format_opensearch_id_column='a', output_format_opensearch_index_column='b', output_format_opensearch_include_index_column_in_document=true;
select 'a' as a, 'b' as b, 1 as c, 1.1 as d format OpenSearch settings output_format_opensearch_id_column='a', output_format_opensearch_include_id_column_in_document=true, output_format_opensearch_index_column='b', output_format_opensearch_include_index_column_in_document=true;
