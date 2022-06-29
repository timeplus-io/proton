import os, sys, logging, time,uuid, datetime, socket,logging
from confluent_kafka import Producer, Consumer
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import json, requests
import multiprocessing as mp

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)


    

#Cryptor coins prices https://api.blockchain.com/v3/exchange/tickers/, https://api.blockchain.com/v3/#/
TICKERS_URL = 'https://api.blockchain.com/v3/exchange/tickers/'
#unconfirmed transactions: https://blockchain.info/unconfirmed-transactions?format=json
UNCONFIRMED_TX_URL = 'https://blockchain.info/unconfirmed-transactions?format=json'

DATA_SOURCES = [
    {"id": 0,"name":"blockchain_tickers", "type":"live", "url":'https://api.blockchain.com/v3/exchange/tickers/', "file_source":"tickers.json", "interval":1, }, 
    {"id": 1,"name":"blockchain_transcations", "type":"live", "url":'https://blockchain.info/unconfirmed-transactions?format=json', "file_source":"unconfirmed-transactions.json", "field_2_extract":"txs", "interval":1}, 
    {"id": 2,"name":"citibiken_station", "type":"live", "url":'https://gbfs.citibikenyc.com/gbfs/en/station_status.json',  "file_source":"station_status.json", "interval": 60}, 
    {"id": 3,"name":"covid19", "type":"live", "url":'https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/date-range/2020-01-01/2022-05-30', "file_source":"covid19.json", "field_2_extract":"data", "interval": 1}, 
    {"id": 4,"name":"earthquake", "type":"live", "url":'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojsonp', "file_source":"earthquake.json", "field_2_extract":"features", "interval": 1}, 
    {"id": 5,"name":"iot", "type":"file", "file_source":"iot_devices_small.json", "interval": 1},
    {"id": 6,"name":"github_event", "type":"file", "file_source":"iot_devices_small.json", "interval": 1}
]


CREATE_SOURCE_BODY_TEMPLATE = '''{
   "type":"kafka",
   "connection_config": {
    "auto_create": true,
    "stream_name": "{stream_name}"
  },
   "name": "{source_name}",
   "properties":{
      "data_type":"json",
      "offset":"latest",
      "sasl":"none",
      "auto_field_extraction":true,
      "schema_registry_address":"",
      "schema_registry_api_key":"",
      "schema_registry_api_secret":"",
      "tls":{
         "disable":true,
         "skip_verify_server":false
      },
      "brokers":"{broker}",
      "topic":"{topic_name}",
      "group":"test"
   },
   "size":3
}'''

SOURCE_URL_TEMPLATE = 'http://{host}:{port}/api/v1beta1/sources/'
START_SOURCE_URL_TEMPLATE = 'http://{host}:{port}/api/v1beta1/sources/{source_id}/start'
NEUTRON_HOST = 'localhost'
NEUTRON_PORT = '8000'

def acked(err, msg):
    if err is not None:
        logger.debug("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        logger.debug("Message produced: %s" % (str(msg))) 


def get_2_kafka(get_url, kafka_conf, topic_name, interval=5):
    producer = Producer(conf)
    #for i in range(10):
    #    producer.produce('test', key=f"{i}", value="value", callback=acked)
    #print("produce done.")
    if topic_name == None:
        topic_name = str(uuid.uuid1())    
    i = 0
    session = requests.Session()
    while True:
        res = session.get(get_url)
        item_list = res.json()        
        for item in ticker_list:
            producer.produce(topic_name, key=f"{i}", value=json.dumps(item), callback=acked)
            time.sleep(interval)
            i += 1
        producer.poll(1)
        #time.sleep(interval)


def produce_exec_json(producer,data_json, topic_name, interval):
    i = 0
    if not isinstance(data_json, list):
        for key in data_json:
            item = {'data':{key:data_json[key]}}
            producer.produce(topic_name, key=f"{i}", value=json.dumps(item), callback=acked)
            time.sleep(interval)
            i += 1
        producer.poll(1)
    else:
        for item in data_json:
            producer.produce(topic_name, key=f"{i}", value=json.dumps(item), callback=acked)
            time.sleep(interval)
            i += 1
        producer.poll(1)     

def produce_from_data_source(data_source, kafka_conf, topic_name):
    logger = mp.get_logger()     
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)   
    logger.setLevel(logging.DEBUG)      
    name = data_source.get("name")
    file_source = data_source.get('file_source')
    interval = data_source.get('interval')
    field_2_extract = data_source.get('field_2_extract')
    logger.debug(f"file_source = {file_source}, interval = {interval}, filed_2_extract = {field_2_extract}")
    producer = Producer(kafka_conf)
    with open(file_source) as f:
        data_json = json.load(f, strict=False)
        logger.debug(f"data soruce file = {file_source}, was succesfully loaded.")
    
    while True:
        #res = session.get(get_url)
        #item_list = res.json()
        if field_2_extract is not None:
            data_for_ingest = data_json[field_2_extract]
            produce_exec_json(producer,data_for_ingest, topic_name, interval)                                      
        else:
            produce_exec_json(producer, data_json, topic_name, interval)
       
        
        #time.sleep(interval)        

def kafka_source_create(session, neutron_host, neutron_port, kafka_conf, source_url_template, start_source_url_template, create_source_body_template, data_source_list, number_2_create):
    sources = []
    procs = []
    j = 0
    for i in range(number_2_create):
        data_source = data_source_list[j]
        name = data_source.get("name")
        source_create_url = source_url_template.replace('{host}', neutron_host)
        source_create_url = source_create_url.replace('{port}', neutron_port)
        start_source_url = start_source_url_template.replace('{host}', neutron_host)
        start_source_url = start_source_url.replace('{port}', neutron_port)
        source_name = 'source_'+ str(i) + '_' + name
        topic_name = source_name
        stream_name = 's_'+ str(i) +'_' + name
        broker = kafka_conf.get("bootstrap.servers")
        print(f"source_name = {source_name}")
        create_source_body = create_source_body_template.replace('{source_name}', source_name)
        create_source_body = create_source_body.replace('{stream_name}', stream_name)
        create_source_body = create_source_body.replace('{broker}', broker)
        create_source_body = create_source_body.replace('{topic_name}', topic_name)
        create_source_body = json.loads(create_source_body)
        create_source_body = json.dumps(create_source_body)

        args = (
            data_source, 
            kafka_conf, 
            topic_name
        )

        proc = mp.Process(target = produce_from_data_source, args = args)
        proc_info = {
            "proc": proc,
            "source_create_url": source_create_url,
            "start_source_url": start_source_url,
            "create_source_body": create_source_body
            
        }
        procs.append(proc_info)
        proc.start()

        j += 1
        if j >= len(data_source_list):
            j = 0

    for i in range(len(procs) if len(procs) <= 60 else 60):
        time.sleep(1)

    for proc in procs:
        time.sleep(5) #wait the kafka topic created successfully
        print(f"source_create_url = {proc['source_create_url']}, create_source_body = {proc['create_source_body']}")
        res = session.post(proc['source_create_url'], data=proc['create_source_body'])
        print(f"res.status_code = {res.status_code}")
        if res.status_code == 201:
            source_id = res.json().get('id')
            start_source_url = proc['start_source_url'].replace('{source_id}', source_id)
            print(f"start_source_url = {start_source_url}")
            res = session.post(start_source_url)
            print(f"res.status_code = {res.status_code}")        
    
    for proc in procs:
        proc['proc'].join()    


if __name__ == "__main__":
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)    
    #todo: use a config file for setting kafka 
    #parser = ArgumentParser()
    #parser.add_argument('config_file', type=FileType('r'))
    #args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    #config_parser = ConfigParser()
    #config_parser.read_file(args.config_file)
    
    conf = {
        'bootstrap.servers': 'kafka:29092',
        'client.id': socket.gethostname()
    }

    session = requests.Session()

    data_source_list = []
    data_source_list.append(DATA_SOURCES[5])


    kafka_source_create(session, NEUTRON_HOST, NEUTRON_PORT, conf, SOURCE_URL_TEMPLATE, START_SOURCE_URL_TEMPLATE, CREATE_SOURCE_BODY_TEMPLATE, data_source_list, 10)


    #data_source = DATA_SOURCES[1]
    #produce_from_data_source(data_source, conf, 'txs')



    #get_2_kafka(TICKERS_URL, conf, 'test1')

'''
    producer = Producer(conf)
    #for i in range(10):
    #    producer.produce('test', key=f"{i}", value="value", callback=acked)
    #print("produce done.")
    

    i = 0
    session = requests.Session()
    while True:
        res = session.get(TICKERS_URL)
        ticker_list = res.json()        
        for ticker in ticker_list:
            producer.produce('test', key=f"{i}", value=json.dumps(ticker), callback=acked)
            i += 1
        producer.poll(1)
        time.sleep(2)
'''