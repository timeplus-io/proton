# easy_json_util
tool for ingest json obj json string from github evnet to help to test easy json
* pre-requests: 
install proton-python-driver: s3://tp-internal/proton/proton-python-driver/clickhouse-driver-0.2.4.tar.gz


Usage example:
export GITHUB_TOKEN=......
to ingest data only: python3 ./json_input.py --input_json_files=./event_github.json --stream=event --target_columns=json_string,json_obj --target_columns_types=string,json , target_columns means which columns will be created in the stream to store the github_event data 
to ingest data and query to get latency data about query on a specific column: python3 ./json_input.py --input_json_files=./event_github.json --stream=event --target_columns=json_string,json_obj --query_column=json_obj --mode=latency