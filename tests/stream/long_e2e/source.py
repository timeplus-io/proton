from clickhouse_driver import Client
import os, sys, logging, time,uuid, datetime, socket,logging
from confluent_kafka import Producer, Consumer
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import json, requests, weakref
from numpy import source
import multiprocessing as mp
import threading as td

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

query_list = [
    "select * from $stream_name",
    "select * from $stream_name",
    "SELECT window_end AS time,count(*) AS cnt FROM tumble($stream_name,5s) WHERE _tp_time > to_start_of_hour(now()) GROUP BY window_end SETTINGS seek_to='-1h'", 
    "SELECT window_end,repo, group_array(distinct actor) AS watchers FROM hop($stream_name,1m,10m) WHERE type ='WatchEvent' GROUP BY window_end,repo HAVING length(watchers)>1 emit last 1h",
    "SELECT repo, count(*) FROM $stream_name WHERE type ='WatchEvent' group by repo", 
    "SELECT created_at,repo,json_extract_string(payload,'master_branch') AS master_branch FROM $stream_name WHERE type='CreateEvent'",
    "select now(),repo,count(*) as cnt from hop($stream_name,1s,30m) group by repo,window_end having cnt >5 emit last 1h",
    "select now(),type,count(*) from $stream_name group by type",
    "select now(),count(*) from $stream_name",
    "select window_end,type,count() from tumble($stream_name,1h) group by window_end,type emit last 2d"
]

DATA_SOURCES = [
    {"id": 0,"name":"blockchain_tickers", "type":"live", "url":'https://api.blockchain.com/v3/exchange/tickers/', "file_source":"tickers.json", "interval":1,"data_loader":{"client":"http_get_client"}, 'kafka_stream_num':2, "kafka_producer_thread":2}, 
    {"id": 1,"name":"blockchain_transcations", "type":"live", "url":'https://blockchain.info/unconfirmed-transactions?format=json', "file_source":"unconfirmed-transactions.json", "field_2_extract":"txs", "interval":1, "data_loader":{"client":"http_get_client"}, "kafka_stream_num":2, "kafka_producer_thread":2}, 
    {"id": 2,"name":"citibiken_station", "type":"live", "url":'https://gbfs.citibikenyc.com/gbfs/en/station_status.json',  "file_source":"station_status.json", "interval": 60, "data_loader":{"client":"http_get_client"}, 'kafka_stream_num':2, "kafka_producer_thread":2}, 
    {"id": 3,"name":"covid19", "type":"live", "url":'https://covidtrackerapi.bsg.ox.ac.uk/api/v2/stringency/date-range/2020-01-01/2022-05-30', "file_source":"covid19.json", "field_2_extract":"data", "interval": 1, "data_loader":{"client":"http_get_client"}, "kafka_stream_num":2, "kafka_producer_thread":2}, 
    {"id": 4,"name":"earthquake", "type":"live", "url":'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojsonp', "file_source":"earthquake.json", "field_2_extract":"features", "interval": 1, "data_loader":{"client":"http_get_client"}, "kafka_stream_num":2, "kafka_producer_thread":2}, 
    {"id": 5,"name":"iot", "type":"file", "file_source":"iot_devices_small.json", "interval": 1, "data_loader":{"client":"http_get_client"}, "kafka_stream_num":2, "kafka_producer_thread":2},
    {"id": 6,"name":"github_event", "type":"file", "file_source":"iot_devices_small.json", "interval": 1, "data_loader":{"client":"git_event_client", "config":{"ENVs":[{"$GITHUB_TOKEN":None}]}}, "kafka_stream_num":2, "kafka_producer_thread":2}
]
  


class Source():
    def __init__(self, host, port, **source_context): #source_context is a dict of source_url, start_url, source_create_body and etc.
        self._host = host
        self._port = str(port)
        self._source_context = source_context
    @property
    def host(self):   
        if self._host is None:
            return self._host
        else:
            return None
    @property
    def port(self):   
        if self._port is None:
            return self._port
        else:
            return None
    @property
    def source_context(self):   
        if self._source_context is None:
            return self._source_context
        else:
            return None


class TimeplusKafkaSource(Source): #todo: make the source creation more flexible to support different type of sources
    SOURCE_URL_TEMPLATE = 'http://{host}:{port}/api/v1beta1/sources/'
    START_SOURCE_URL_TEMPLATE = 'http://{host}:{port}/api/v1beta1/sources/{source_id}/start'
    CREATE_KAFKA_SOURCE_BODY_TEMPLATE = '''{
    "type":"kafka",
    "connection_config": {
        "auto_create": true,
        "stream_name": "{stream_name}"
    },
    "name": "{source_name}",
    "properties":{
        "data_type":"{data_type}",
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

    def __init__(self, host, port, **source_context):
        self.id = None
        self.status = None #None, Failed, Init, Run
        super().__init__(host, port, **source_context)
    
    def create(self, session):
        source_create_url =TimeplusKafkaSource.SOURCE_URL_TEMPLATE.replace('{host}', self._host)
        source_create_url = source_create_url.replace('{port}', self._port)
        logger.debug(f"self._source_context = {self._source_context}")
        source_name = topic_name = self._source_context.get("topic_name")
        data_type = self._source_context.get("data_type")
        broker = self._source_context.get("broker")
        stream_name = source_name
        print(f"source_name = {source_name}")
        create_source_body = TimeplusKafkaSource.CREATE_KAFKA_SOURCE_BODY_TEMPLATE.replace('{source_name}', source_name)
        create_source_body = create_source_body.replace('{stream_name}', stream_name)
        create_source_body = create_source_body.replace('{broker}', broker)
        create_source_body = create_source_body.replace('{topic_name}', topic_name)
        create_source_body = create_source_body.replace('{data_type}', data_type)
        create_source_body = json.loads(create_source_body)
        create_source_body = json.dumps(create_source_body)        
        print(f"source_create_url = {source_create_url}, create_source_body = {create_source_body}")
        res = session.post(source_create_url, data=create_source_body)
        print(f"res.status_code = {res.status_code}")
        if res.status_code == 201:
            source_id = res.json().get('id')
            self.id = source_id 
            return source_id
        else:
            self.status = 'Failed'
            return None        
    
    def start(self, session, source_id):
        start_source_url = TimeplusKafkaSource.START_SOURCE_URL_TEMPLATE.replace('{host}', self._host)
        start_source_url = start_source_url.replace('{port}', self._port)
        start_source_url = start_source_url.replace('{source_id}', source_id)
        print(f"start_source_url = {start_source_url}")
        res = session.post(start_source_url)
        print(f"res.status_code = {res.status_code}")                 

    def create_and_start(self, session):
        source_id = self.create(session)
        if source_id is not None:
            self.start(session, source_id)



class KafkaSource(Source):
    def __init__(self, host, port, topic_name, **source_context):
        self.topic_name = topic_name
        logger.debug(f"source_context={source_context}")
        super().__init__(host, port, **source_context)
        self.bootstrap_server = f'{host}:{port}' #todo: support bootstrap_servers
        self._kafka_conf = {
            'bootstrap.servers': self.bootstrap_server,
            'client.id': socket.gethostname()
            }
        self.producer = Producer(self._kafka_conf)
        print(f"KafkaSource init, self._host={self._host}, self._port={self._port}, self.topic_name={self.topic_name}, self._kafka_conf={self._kafka_conf}, self._source_context={self._source_context}")

    
    def _acked(self, err, msg):
        if err is not None:
            logger.debug("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            logger.debug("Message produced: %s" % (str(msg)))    

    def _produce_exec_json(self,producer,data_json, topic_name, interval):
        logger = mp.get_logger()
        i = 0
        if not isinstance(data_json, list):
            for key in data_json:
                item = {'data':{key:data_json[key]}}
                producer.produce(topic_name, key=f"{i}", value=json.dumps(item), callback=self._acked)
                time.sleep(interval)
                i += 1
            producer.poll(1)
        else:
            for item in data_json:
                producer.produce(topic_name, key=f"{i}", value=json.dumps(item), callback=self._acked)
                time.sleep(interval)
                i += 1
            producer.poll(1) 
    
    def _produce_from_data_source(self):
        logger = mp.get_logger()
        print(f'topic_name = {self.topic_name}, start to produce......')
        logging_level = os.getenv("TIMEPLUS_MULTI_SOURCE_DEBUG_LEVEL", "INFO")
        if logging_level == "DEBUG":   #todo: support multiple logging level from logconf
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)         
        logger = mp.get_logger()     
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.formatter = formatter
        logger.addHandler(console_handler)   
        logger.setLevel(logging_level)      
        name = self._source_context.get("name")
        file_source = self._source_context.get("file_source")
        interval = self._source_context.get('interval')
        field_2_extract = self._source_context.get('field_2_extract')
        logger.debug(f"file_source = {file_source}, interval = {interval}, filed_2_extract = {field_2_extract}")
        with open(file_source) as f:
            data_json = json.load(f, strict=False)
            logger.debug(f"data soruce file = {file_source}, was succesfully loaded.")
        
        while True:
            #res = session.get(get_url)
            #item_list = res.json()
            if field_2_extract is not None:
                data_for_ingest = data_json[field_2_extract]
                self._produce_exec_json(self.producer,data_for_ingest, self.topic_name, interval)                                      
            else:
                self._produce_exec_json(self.producer, data_json, self.topic_name, interval)    
    def produce_from_data_source(self):
        logger = mp.get_logger()
        num_of_producer_thread = self._source_context.get("kafka_producer_thread")
        if num_of_producer_thread is None:
            num_of_producer_thread = 1
        print(f"topic_name = {self.topic_name}, num_of_producer_thread = {num_of_producer_thread}, to start producer threads......")
        threads = []
        for i in range(num_of_producer_thread):
            t = td.Thread(target = self._produce_from_data_source)
            threads.append(t)
            t.start()
        for t in threads:
            t.join()


class TplusSourceFactory():
    def __init__(self, kafka_host, kafka_port, neutron_host, neutron_port, *source_context_list):
        self.source_context_list = source_context_list
        self.num_of_source_2_create = 0
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.neutron_host = neutron_host
        self.neutron_port = neutron_port
        self.session = requests.Session()
        self.kafka_sources = []
        self.tplus_sources = []
        logger.debug(f"self.neutron_host = {self.neutron_host}")

    @classmethod
    def create_kafka_source(cls, kafka_host, kafka_port, topic_name, data_source):
        kafka_source = KafkaSource(kafka_host, kafka_port, topic_name, **data_source)
        kafka_source.produce_from_data_source()
    
    def _create_kafka_sources(self, num_of_source_2_create): #todo: extend multi threading support on produce
        sources = []
        procs = []
        j = 0
        for i in range(num_of_source_2_create):
            data_source = self.source_context_list[j]
            name = data_source.get("name")
            data_type = data_source.get("data_type")
            if data_type is None:
                data_type = 'json' #if data_type is not set, json by default
            source_name = 'source_'+ str(i) + '_' + name
            topic_name = source_name
            args = (
                self.kafka_host,
                self.kafka_port,
                topic_name,
                data_source
            )
            proc = mp.Process(target = self.create_kafka_source, args = args)
            proc_info = {
                "topic_name": topic_name,
                "data_type": data_type,
                "proc":proc
            }
            self.kafka_sources.append(proc_info)
            #proc.start()
            j += 1
            if j >= len(self.source_context_list):
                j = 0
                
    
    def create_and_start_tplus_source(self, **tplus_source_context):
        tplus_source = TimeplusKafkaSource(self.neutron_host, self.neutron_port, **tplus_source_context)
        tplus_source.create_and_start(self.session)
        return tplus_source

    def create_and_start_tplus_sources_from_kafka_sources(self):
        for proc in self.kafka_sources:
            topic_name = proc.get('topic_name')
            data_type = proc.get('data_type')
            broker = f"{self.kafka_host}:{self.kafka_port}"
            tplus_source_context = {
                "topic_name": topic_name,
                "data_type": "json",
                "broker": broker
            }                
            tplus_source = self.create_and_start_tplus_source(**tplus_source_context)
            
            self.tplus_sources.append(tplus_source)
    
    def create_and_start_all(self, num_of_source_2_create):
        self._create_kafka_sources(num_of_source_2_create)
        logger.debug(f"self.kafka_sources = {self.kafka_sources}")
        for proc in self.kafka_sources:
            proc['proc'].start()

        for i in range(len(self.kafka_sources) if len(self.kafka_sources) <= 60 else 60):
            time.sleep(1)    
        
        self.create_and_start_tplus_sources_from_kafka_sources()
        for proc in self.kafka_sources:
            proc['proc'].join()        

    def creat_and_start_kafka_sources(self, num_of_source_2_create):
        self._create_kafka_sources(self, num_of_source_2_create)
        
        for proc in self.kafka_sources:
            proc['proc'].start()        
        
        for proc in self.kafka_sources:
            proc['proc'].join



if __name__ == "__main__":
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)
    os.environ["TIMEPLUS_MULTI_SOURCE_DEBUG_LEVEL"] = "DEBUG"
    logging_level = os.getenv("TIMEPLUS_MULTI_SOURCE_DEBUG_LEVEL", "INFO")
    if logging_level == "DEBUG":   #todo: support multiple logging level from logconf
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)    
    #todo: use a config file for setting kafka 
    conf = {
        'bootstrap.servers': 'kafka:29092',
        'client.id': socket.gethostname()
    }

    session = requests.Session()

    data_source_list = []
    data_source_list.append(DATA_SOURCES[5])
    logger.debug(f"DATA_SOURCES[5]={DATA_SOURCES[5]}")

    tplus_source_factory = TplusSourceFactory('kafka', 29092, 'localhost', 8000, *data_source_list)
    tplus_source_factory.create_and_start_all(2)
    
