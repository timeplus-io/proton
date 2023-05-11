import os, sys, logging, datetime, time, uuid, traceback, json, requests
import swagger_client
from enum import Enum, unique
from swagger_client.rest import ApiException

import timeplus
from timeplus import Environment, Stream

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

@unique
class TPRestVersion(Enum):
    V1BETA1 = "v1beta1"
    V1BETA2 = "v1beta2"

class TPRest(object):
    def __init__(self, address, api_key, resource_path, version = TPRestVersion.V1BETA1, **kwargs):
        self.address = address
        self.api_key = api_key
        self.uri = address + '/' + 'api' + '/' + version.value + resource_path
        self.version = version
        self.kwargs = kwargs
        self.headers = {
            'X-Api-Key': api_key
        }
    
    def get(self, **kwargs):
        logger.debug(f"kwargs = {kwargs}")
        path_params = "?"
        session = requests.Session()
        uri = self.uri
        if kwargs: #if kwargs is not empty, put the kwargs into path params
            for key in kwargs.keys():
                path_params += str(key) + '=' + str(kwargs[key])
            uri += path_params
        logger.debug(f"uri = {uri}")
        response = session.get(uri, headers=self.headers)
        return response


    def post(self, payload_dict):
        logger.debug(f"uri = {self.uri, }body = {payload_dict}")
        session = requests.Session()
        payload = json.dumps(payload_dict)
        headers = self.headers
        headers['Content-Type'] = 'application/json'
        response = session.post(self.uri, data=payload, headers=headers)
        return response

    def delete(self):
        pass
    
    def patch(self, body):
        pass
 

class TPDataObjRest(TPRest):
    def __init__(self, address, api_key, resoruce_path, properties_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
        self.resource_path = resoruce_path
        self.properties_dict = properties_dict
        
        super().__init__(address, api_key, self.resource_path, version)
    
    def list(self):
        try:
            obj_list = []
            res = super().get()
            logger.debug(f"resource_path = {self.resource_path}, res = {res}, status_code = {res.status_code}")
            if res.status_code == 200:
                view_list = res.json()
            return view_list
        except(BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return error


    def get(self):
        if self.properties_dict is None:
            logger.debug(f"resource_path = {self.resource_path}, no Timeplus data obj properties specified, obj could be gotten")
            return None
        res = super().get(**self.properties_dict)
        return res
    

    def create(self):
        if self.properties_dict is None or not self.properties_dict:
            logger.debug(f"resource_path = {self.resource_path}, properties of Timeplus data obj are not specified, obj could not be created.")
            return None
        try:
            res = self.post(self.properties_dict)
            return res
        except(BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return error   


class TPStreamRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
        self.resource_path = '/streams'
        self.properties_dict = properties_dict
        
        super().__init__(address, api_key, self.resource_path,properties_dict, version) 

class TPSourceRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
        self.resource_path = '/sources'
        self.properties_dict = properties_dict
        
        super().__init__(address, api_key, self.resource_path,properties_dict, version)          

class TPSinkRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
        self.resource_path = '/sinks'
        self.properties_dict = properties_dict
        
        super().__init__(address, api_key, self.resource_path,properties_dict, version) 


class TPTopoRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version =  TPRestVersion.V1BETA2): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
        self.resource_path = '/topology'
        self.properties_dict = properties_dict
        
        super().__init__(address, api_key, self.resource_path,properties_dict, version) 


class TPViewRest(TPRest):
    def __init__(self, address, api_key, view_paras_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
        self.view_paras_dict = view_paras_dict
        self.resource_path = '/views'
        super().__init__(address, api_key, self.resource_path, version)
    
    def list(self):
        try:
            view_list = []
            res = super().get()
            logger.debug(f"res = {res}, status_code = {res.status_code}")
            if res.status_code == 200:
                view_list = res.json()
            return view_list
        except(BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return ImportError


    def get(self):
        if self.name is None:
            logger.debug(f"no name attribute, view could be gotten")
            return None
        kwargs = {}
        kwargs = {'name': self.name}
        res = super().get(**kwargs)
        return res
    

    def create(self):
        if self.view_paras_dict is None or not self.view_paras_dict:
            logger.debug(f"parameters of view are not specified, view could not be created.")
            return None
        try:
            res = self.post(self.view_paras_dict)
            return res
        except(BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return None

class TPUdfRest(TPRest):
    def __init__(self, address, api_key, udf_paras_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
        logger.debug(f"udf_paras_dict = {udf_paras_dict}")
        self.resource_path = '/udfs'
        self.udf_paras_dict = udf_paras_dict
        super().__init__(address, api_key, self.resource_path, version)
   
    def list(self):
        try:
            udf_list = []
            res = super().get()
            logger.debug(f"res = {res}, status_code = {res.status_code}")
            if res.status_code == 200:
                udf_list = res.json()
            return udf_list
        except(BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return udf_list


    def get(self):
        if self.name is None:
            logger.debug(f"no name attribute, view could be gotten")
            return None
        kwargs = {}
        kwargs = {'name': self.name}
        res = super().get(**kwargs)
        return res
    

    def create(self):
        if self.udf_paras_dict is None or not self.udf_paras_dict:
            logger.debug(f"parameters of udf are not specified, UDF could not be created.")
            return None
        try:
            res = self.post(self.udf_paras_dict)
            return res
        except(BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return None



if __name__ == '__main__':

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)

    logger.setLevel(logging.DEBUG)

    api_key = os.environ.get("TIMEPLUS_API_KEY")
    address = 'http://localhost:8000'
    env = Environment().address(address).apikey(api_key)
    stream_list = Stream(env=env).list()
    #print(f"stream_list = {stream_list}")


    stream_properties =     {
        "created_by": {
            "name": "Jove Zhong",
            "id": "google-oauth2|109517076194156682691"
        },
        "created_at": "2023-04-18 18:14:17",
        "last_updated_by": {
            "name": "Jove Zhong",
            "id": "google-oauth2|109517076194156682691"
        },
        "last_updated_at": "2023-04-18 18:14:17",
        "name": "lpn_list_copy",
        "engine": "Stream",
        "ttl": "to_datetime(_tp_time) + INTERVAL 7 DAY",
        "columns": [
            {
                "name": "lpn",
                "type": "string",
                "nullable": False,
                "default": ""
            },
            {
                "name": "_tp_time",
                "type": "datetime64(3, 'UTC')",
                "nullable": False,
                "default": "now64(3, 'UTC')"
            }
        ],
        "logstore_retention_bytes": 10737418240,
        "logstore_retention_ms": 604800000,
        "is_external": False,
        "description": ""
    }

    tp_stream = TPStreamRest(address, api_key, stream_properties)
    res = tp_stream.create()
    print(f"res = {res}")
    
    # view_params_dict =  {
    #     "created_by": {
    #         "name": "Qijun Niu",
    #         "id": "google-oauth2|101711575418688743483"
    #     },
    #     "created_at": "2022-12-14 07:33:27",
    #     "last_updated_by": {
    #         "name": "Qijun Niu",
    #         "id": "google-oauth2|101711575418688743483"
    #     },
    #     "last_updated_at": "2022-12-14 07:33:27",
    #     "name": "mv_fatigue_24h_alert_copy",
    #     "description": "",
    #     "query": "WITH alerts AS(\nSELECT lpn,window_start as windowStart, window_end as windowEnd, count_if(state='RUNNING') as drivingMinute FROM hop(mv_truck_state,5m,24h) GROUP BY lpn,window_start, window_end\nHAVING drivingMinute>(16*60) settings seek_to = '-24h')\nSELECT * FROM dedup(alerts,lpn,600s) -- avoid duplicated truck id within 10min",
    #     "materialized": True,
    #     "target_stream": "",
    #     "ttl": "",
    #     "logstore_retention_bytes": 10737418240,
    #     "logstore_retention_ms": -1,
    #     "columns": [
    #         {
    #             "name": "lpn",
    #             "type": "string",
    #             "nullable": False,
    #             "default": ""
    #         },
    #         {
    #             "name": "windowStart",
    #             "type": "datetime64(3, 'UTC')",
    #             "nullable": False,
    #             "default": ""
    #         },
    #         {
    #             "name": "windowEnd",
    #             "type": "datetime64(3, 'UTC')",
    #             "nullable": False,
    #             "default": ""
    #         },
    #         {
    #             "name": "drivingMinute",
    #             "type": "uint64",
    #             "nullable": False,
    #             "default": ""
    #         }
    #     ]
    # }

    # tp_view = TPViewRest(address, api_key, view_params_dict)
    # res = tp_view.create()

    exit(0)
    # resource_path = '/views'
    # tp_view = TPViewRest(address,api_key,resource_path)
    view_name = "mv_fatigue_4h_alert_copy1"
    query = "WITH truck_rest AS\n  (\n    SELECT\n      window_start AS windowStart, window_end AS windowEnd, lpn, if(count_if(state = 'IDLE') = 40, 'IDLE_20MIN', 'OFF_20MIN') AS rest_state\n    FROM\n hop(mv_fatigue_state, 1m, 20m)\n    GROUP BY\n      window_start, window_end, lpn\n    HAVING\n      (count_if(state = 'IDLE') = 40) OR (count_if(state = 'OFF') = 40)\n    SETTINGS\n      seek_to = '-6h'\n  ), alerts AS\n  (\n    SELECT\n      window_start AS windowStart, window_end AS windowEnd, lpn, count_if(rest_state = 'IDLE_20MIN') AS idle_count, count_if(rest_state = 'OFF_20MIN') AS off_count\n    FROM\n hop(truck_rest, windowEnd, 1m, 6h)\n    GROUP BY\n      window_start, window_end, lpn\n    HAVING\n      (idle_count = 0) AND (off_count = 0)\n    EMIT TIMEOUT 1m\n  )\nSELECT\n  uuid() AS id, *\nFROM\n dedup(alerts, lpn, 1200s)"
    materialized = True    
    tp_view = TPViewRest(address,api_key, view_name, query)
    view_list_response = tp_view.list()

    print(f"view_response = {view_list_response}")
    udf_paramethers_dict = {
        "created_by": {
            "name": "Gang Tao",
            "id": "google-oauth2|112510942135182208745"
        },
        "created_at": "2023-01-24 19:44:11",
        "last_updated_by": {
            "name": "Gang Tao",
            "id": "google-oauth2|112510942135182208745"
        },
        "last_updated_at": "2023-01-24 19:44:11",
        "type": "javascript",
        "name": "myrank",
        "arguments": [
            {
                "name": "arg0",
                "type": "float"
            },
            {
                "name": "arg1",
                "type": "integer"
            }
        ],
        "return_type": "integer",
        "auth_context": {
            "key_name": "",
            "key_value": ""
        },
        "is_aggregation": True,
        "source": "{\n    initialize: function() {\n        this.ranking = [];\n    },\n\n    process: function(values, groups) {\n        const ranking = (arr) => arr.map((x, y, z) => z.filter((w) => w > x).length + 1);\n        const groupRanking = (arr, n) =>  arr.map( (x) => Math.ceil(x/n));\n        const group = groups[0];\n        this.ranking = groupRanking(ranking(values),group);\n    },\n\n    finalize: function() {\n        return this.ranking;\n    }\n};",
        "description": ""
    }

    udf = TPUdfRest(address, api_key, udf_paramethers_dict)

    udf_list_reponse = udf.list()
    print(f"udf_list_response = {udf_list_reponse}")

    udf_create_response = udf.create()
    print(f"udf_create_response = {udf_create_response}")

    #print(f"view_response_json = {view_response_json}")
    # for view in view_response:
    #     print(f'name = {view["name"]}')
    #     print(f'query = {view["query"]}')
    
    #view_create_response = tp_view.create()
    #logging.debug(f"view_create_response = {view_create_response}")

    # view_res = tp_view.get()
    # logger.debug(f"view_res = {view_res}")
    # if view_res.status_code == 200:
    #     logger.debug(f"view_name = {view_name}, view_res.json() = {view_res.json()}")




    # payload = {
    #     "name":view_name,
    #     "query":query,
    #     "materialized": materialized
    # }

    # view_create_response = tp_view.post(payload)
    # edges = topo_response.edges
    # nodes = topo_response.nodes
    # for edge in edges:
    #     print(edge)
    #     print(edge.to_dict().get("source"))
    