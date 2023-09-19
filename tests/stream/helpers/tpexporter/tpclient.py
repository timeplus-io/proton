import os
import sys
import logging
import datetime
import time
import uuid
import traceback
import json
import requests
# import swagger_client
from enum import Enum, unique
# from swagger_client.rest import ApiException
import timeplus
from timeplus import Environment, Stream
import sseclient
from proton_driver import Client

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

DEFAULT_REST_GET_TIMEOUT = 5
DEFAULT_REST_TIMEOUT_RETRY = 3
DEFAULT_REST_TIMEOUT_RETRY_INTERVAL = 1
TPNATIVE_DRIVER = 'proton_driver'


@unique
class TPRestVersion(Enum):
    V1BETA1 = "v1beta1"
    V1BETA2 = "v1beta2"


@unique
class SSEEvent(Enum):
    QUERY = "query"
    Metrics = "metrics"
    MESSAGE = "message"


@unique
class ProtocolType(Enum):
    READ = "read_protocol"
    WRITE = "write_protocol"
    # TPNativeRest = "tp_native_rest"


@unique
class TPProtocol(Enum):
    TPRest = "tp_rest"
    TPNative = "tp_native"
    TPFile = "tp_file"
    # TPNativeRest = "tp_native_rest"


@unique
class NativeProtocol(Enum):
    HOST = "tp_native_host"
    PORT = "tp_native_port"
    REST_PORT = "tp_native_rest_port"


@unique
class DataObjType(Enum):
    SOURCE = "source"
    STREAM = "stream"
    VIEW = "view"
    SINK = "sink"
    UDF = "udf"
    LINEAGE = "lineage"


ResourcePath = {
    DataObjType.SOURCE.value: "/sources",
    DataObjType.STREAM.value: "/streams",
    DataObjType.VIEW.value: "/views",
    DataObjType.SINK.value: "/sinks",
    DataObjType.UDF.value: "/udfs",
    DataObjType.LINEAGE.value: "/topology"
}


class TPRestBase(object):
    def __init__(self, uri, headers={}, **properties_dict):
        self._uri = uri
        self._headers = headers
        self._properties_dict = properties_dict
        # name = self._properties_dict.get("name")
        # if name is not None:
        #     self._ingest_uri = self._uri + '/' + name + '/ingest'
        # else:
        #     self._ingest_url = None

    def create(self, uri=None):
        res = ''
        if self._properties_dict is None or not self._properties_dict:
            logger.debug(
                f"self._uri = {self._uri}, specified uri = {uri}, properties of Timeplus data obj are not specified, obj could not be created.")
            return None
        try:
            if uri is not None:
                res = self.post(self._properties_dict, uri)
            else:
                res = self.post(self._properties_dict)
            return res
        except (BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return error

    def ingest(self, payload, uri=None):  # ingest is only valid for stream
        res = ''
        if uri is None and self._uri is None:
            raise Exception(f"no ingest uri, ingestion failed.")
        try:
            if uri is not None:
                res = self.post(payload, uri)
            else:
                res = self.post(payload)
            return res
        except (BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error

    def delete(self, uri=None):
        logger.debug(f"uri = {uri}, self._uri = {self._uri}, deleting...")
        if uri is None and self._uri is None:
            raise Exception(f"no delete uri, delete failed.")
        self._response = None
        try:
            if uri is not None:
                delete_uri = uri
            else:
                delete_uri = self._uri
            logger.debug(
                f"delete uri = {delete_uri}, self._headers = {self._headers}")
            with requests.Session() as session:
                self._response = session.delete(
                    delete_uri, headers=self._headers)
            return self._response
        except (BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error

    def get(self, **kwargs):
        # logger.debug(f"kwargs = {kwargs}")
        path_params = "?"
        timeout = self._properties_dict.get(
            'rest_get_timeout', DEFAULT_REST_GET_TIMEOUT)
        max_retry_times = self._properties_dict.get(
            'rest_timeout_retry', DEFAULT_REST_TIMEOUT_RETRY)
        retry_interval = self._properties_dict.get(
            'rest_timeout_retry_interval', DEFAULT_REST_TIMEOUT_RETRY_INTERVAL)
        if kwargs:  # if kwargs is not empty, put the kwargs into path params
            for key in kwargs.keys():
                path_params += str(key) + '=' + str(kwargs[key])
            self._uri += path_params
        logger.debug(f"self._uri = {self._uri}")
        # session = requests.Session()
        self._response = None
        retry_count = 0
        retry = True
        while retry_count < max_retry_times and retry:
            try:
                with requests.Session() as session:
                    self._response = session.get(
                        self._uri, headers=self._headers, timeout=timeout)
                retry = False
            except (BaseException) as error:
                logger.debug(f"Exception, error = {error}")
                traceback.print_exc()
                self._response = error
            retry_count += 1
            if retry:
                logger.debug(
                    f"rest get timeout retry: max_retry_times = {max_retry_times}, retry_count = {retry_count}")
            time.sleep(retry_interval)  # wait for 1 second to retry
        return self._response

    def post(self, payload_dict, uri=None):
        logger.debug(f"uri = {uri}, self._uri = {self._uri}")
        # session = requests.Session()
        self._response = None
        payload = None
        try:
            payload = json.dumps(payload_dict)
            headers = self._headers
            headers['Content-Type'] = 'application/json'
            with requests.Session() as session:

                if uri is None:
                    uri = self._uri
                self._response = session.post(
                    uri, data=payload, headers=headers)
                if self._response.status_code != 200:
                    print(
                        f"TPRest post failed, response status code = {self._response.status_code}, \n post_uri = {uri}, post_body = {payload}, \nresponse content = {self._response.content}")
            return self._response
        except (Exception) as error:
            print(f"BaseException, error={error}")
            traceback.print_exc()
            return error

    def patch(self, body):
        pass

# class TPRest(TPRestBase):
#     def __init__(self, address, api_key, resource_path, version = TPRestVersion.V1BETA1, **properties):
#         self.address = address
#         self.api_key = api_key
#         self.resource_path = resource_path
#         self.uri = address + '/' + 'api' + '/' + version.value + resource_path
#         self.version = version
#         self.properties_dict = properties
#         self.headers = {
#             'X-Api-Key': api_key
#         }
#         super().__init__(self.uri, **self.properties_dict)

    # def get(self, **kwargs):
    #     logger.debug(f"kwargs = {kwargs}")
    #     path_params = "?"
    #     #session = requests.Session()
    #     self._response = None
    #     try:
    #         uri = self.uri
    #         if kwargs: #if kwargs is not empty, put the kwargs into path params
    #             for key in kwargs.keys():
    #                 path_params += str(key) + '=' + str(kwargs[key])
    #             uri += path_params
    #         logger.debug(f"uri = {uri}")
    #         with requests.Session() as session:
    #             timeout = self.kwargs.get('timeout')
    #             if timeout is None:
    #                 timeout = DEFAULT_RESPONSE_TIMEOUT # set default timeout
    #             self._response = session.get(uri, headers=self.headers, timeout=timeout)
    #     except(Exception) as error:
    #         logger.debug(f"Exception, error = {error}")
    #         traceback.print_exc()
    #         return error
    #     return self._response

    # def post(self, payload_dict, uri = None):
    #     logger.debug(f"uri = {uri}, self.uri = {self.uri}, body = {payload_dict}")
    #     #session = requests.Session()
    #     self._response = None
    #     try:
    #         payload = json.dumps(payload_dict)
    #         headers = self.headers
    #         headers['Content-Type'] = 'application/json'
    #         with requests.Session() as session:

    #             if uri is None:
    #                 self._response = session.post(self.uri, data=payload, headers=headers)
    #             else:
    #                 self._response = session.post(uri, data=payload, headers=headers)
    #         return self._response
    #     except(Exception) as error:
    #         logger.debug(f"Exception, error = {error}")
    #         traceback.print_exc()
    #         return error

    # def delete(self):
    #     pass

    # def patch(self, body):
    #     pass


class TPDataObjRest(TPRestBase):
    def __init__(self, address, api_key, resource_path, properties_dict, version=TPRestVersion.V1BETA1):
        self._address = address
        self._api_key = api_key
        self._resource_path = resource_path
        self._uri = address + '/' + 'api' + '/' + version.value + resource_path
        self._version = version
        self._properties_dict = properties_dict
        self._headers = {
            'X-Api-Key': api_key
        }
        if isinstance(self._properties_dict, dict):
            super().__init__(self._uri, self._headers, **self._properties_dict)
        else:
            super().__init__(self._uri, self._headers)

    def list(self):
        try:
            obj_list = []
            res = super().get()
            logger.debug(
                f"self._uri = {self._uri}, res = {res}")
            if isinstance(res, requests.Response):
                logger.debug(
                    f"self._uri = {self._resource_path}, res = {res}, status_code = {res.status_code}")
                if res.status_code == 200:
                    obj_list = res.json()
                    return obj_list
                else:
                    logger.debug(
                        f"self._uri = {self._resource_path}, res = {res}, status_code != 200")
                    raise Exception(
                        f"self._uri = {self._resource_path}, res = {res}, status_code != 200")
            elif isinstance(res, BaseException):
                return res
        except (BaseException) as error:
            logger.debug(f"exception, error = {error}")
            traceback.print_exc()
            return error

    def get(self):
        if self.properties_dict is None:
            logger.debug(
                f"self.uri = {self._uri}, no Timeplus data obj properties specified, obj could be gotten")
            return None
        res = super().get(**self.properties_dict)
        return res

    def ingest(self, payload):  # only valid for stream, override TPRestBase for the uri
        try:
            name = self._properties_dict.get("name")
            self._ingest_uri = self._uri + f"/{name}" + "/ingest"
            logger.debug(
                f"self._ingest_uri = {self._ingest_uri}, ingest starts...")
            res = super().ingest(payload, self._ingest_uri)
            return res
        except (BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error

    def delete(self):
        try:
            res = ''
            name = self._properties_dict.get("name")
            self._delete_uri = self._uri + f"/{name}"
            logger.debug(
                f"self._delete_uri = {self._delete_uri}, delete starts...")
            res = super().delete(self._delete_uri)
            return res
        except (BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error

    # def create(self):
    #     if self.properties_dict is None or not self.properties_dict:
    #         logger.debug(f"resource_path = {self.resource_path}, properties of Timeplus data obj are not specified, obj could not be created.")
    #         return None
    #     try:
    #         res = self.post(self.properties_dict)
    #         return res
    #     except(BaseException) as error:
    #         logger.debug(f"exception, error = {error}")
    #         traceback.print_exc()
    #         return error


class TPSSEQueryRest(TPDataObjRest):
    # when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
    def __init__(self, address, api_key, sql, version=TPRestVersion.V1BETA2):
        self.resource_path = '/queries'
        self.properties_dict = {"sql": sql}
        super().__init__(address, api_key, self.resource_path, self.properties_dict, version)

    def query(self):
        res = super().create()
        self._query_res = res
        return self

    def query_result(self):
        query_result_columns = []
        query_result_data = []
        query_result_dict = {
            "columns": query_result_columns, "data": query_result_data}
        try:
            client = sseclient.SSEClient(self._query_res)
            result_columns_extracted = False
            for event in client.events():
                if event.event == SSEEvent.QUERY.value:
                    event_data = json.loads(event.data)
                    # logger.debug(f"event.event = {event.event}, event_data = {event_data}")
                    result_columns = event_data["analysis"]["result_columns"]
                    if not result_columns_extracted:
                        for result_column in result_columns:
                            column = result_column.get("column")
                            query_result_columns.append(column)
                        result_columns_extracted = True

                if event.event == SSEEvent.MESSAGE.value:
                    query_result_data.extend(json.loads(event.data))
            return query_result_dict
        except (BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return None


class TPStreamRest(TPDataObjRest):
    # when create the TPStreamRest, kwargs for the properties for the stream like name = 'stream1', columns = [],
    def __init__(self, address, api_key, properties_dict, version=TPRestVersion.V1BETA1):
        self.resource_path = '/streams'
        self.properties_dict = properties_dict

        super().__init__(address, api_key, self.resource_path, properties_dict, version)


class TPSourceRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version=TPRestVersion.V1BETA1):
        self.resource_path = '/sources'
        self.properties_dict = properties_dict

        super().__init__(address, api_key, self.resource_path, properties_dict, version)


class TPSinkRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version=TPRestVersion.V1BETA1):
        self.resource_path = '/sinks'
        self.properties_dict = properties_dict

        super().__init__(address, api_key, self.resource_path, properties_dict, version)


class TPTopoRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version=TPRestVersion.V1BETA2):
        self.resource_path = '/topology'
        self.properties_dict = properties_dict

        super().__init__(address, api_key, self.resource_path, properties_dict, version)


class TPViewRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version=TPRestVersion.V1BETA1):
        self.resource_path = '/views'
        self.properties_dict = properties_dict

        super().__init__(address, api_key, self.resource_path, properties_dict, version)


class TPUdfRest(TPDataObjRest):
    def __init__(self, address, api_key, properties_dict, version=TPRestVersion.V1BETA1):
        self.resource_path = '/udfs'
        self.properties_dict = properties_dict

        super().__init__(address, api_key, self.resource_path, properties_dict, version)

# class TPViewRest(TPRest):
#     def __init__(self, address, api_key, view_paras_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
#         self.view_paras_dict = view_paras_dict
#         self.resource_path = '/views'
#         super().__init__(address, api_key, self.resource_path, version)

#     def list(self):
#         try:
#             view_list = []
#             res = super().get()
#             logger.debug(f"res = {res}, status_code = {res.status_code}")
#             if res.status_code == 200:
#                 view_list = res.json()
#             return view_list
#         except(BaseException) as error:
#             logger.debug(f"exception, error = {error}")
#             traceback.print_exc()
#             return error


#     def get(self):
#         if self.name is None:
#             logger.debug(f"no name attribute, view could be gotten")
#             return None
#         kwargs = {}
#         kwargs = {'name': self.name}
#         res = super().get(**kwargs)
#         return res


#     def create(self):
#         if self.view_paras_dict is None or not self.view_paras_dict:
#             logger.debug(f"parameters of view are not specified, view could not be created.")
#             return None
#         try:
#             res = self.post(self.view_paras_dict)
#             return res
#         except(BaseException) as error:
#             logger.debug(f"exception, error = {error}")
#             traceback.print_exc()
#             return None

# class TPUdfRest(TPRest):
#     def __init__(self, address, api_key, udf_paras_dict, version = TPRestVersion.V1BETA1): #when create the TPVeiwRest object, kwargs for the properties for the view like name = 'view1', query = 'select * from stream1'
#         logger.debug(f"udf_paras_dict = {udf_paras_dict}")
#         self.resource_path = '/udfs'
#         self.udf_paras_dict = udf_paras_dict
#         super().__init__(address, api_key, self.resource_path, version)

#     def list(self):
#         try:
#             udf_list = []
#             res = super().get()
#             logger.debug(f"res = {res}, status_code = {res.status_code}")
#             if res.status_code == 200:
#                 udf_list = res.json()
#             return udf_list
#         except(BaseException) as error:
#             logger.debug(f"exception, error = {error}")
#             traceback.print_exc()
#             return udf_list


#     def get(self):
#         if self.name is None:
#             logger.debug(f"no name attribute, view could be gotten")
#             return None
#         kwargs = {}
#         kwargs = {'name': self.name}
#         res = super().get(**kwargs)
#         return res


#     def create(self):
#         if self.udf_paras_dict is None or not self.udf_paras_dict:
#             logger.debug(f"parameters of udf are not specified, UDF could not be created.")
#             return None
#         try:
#             res = self.post(self.udf_paras_dict)
#             return res
#         except(BaseException) as error:
#             logger.debug(f"exception, error = {error}")
#             traceback.print_exc()
#             return None


TPRestClass = {
    DataObjType.SOURCE.value: TPSourceRest,
    DataObjType.STREAM.value: TPStreamRest,
    DataObjType.VIEW.value: TPViewRest,
    DataObjType.SINK.value: TPSinkRest,
    DataObjType.UDF.value: TPUdfRest,
    DataObjType.LINEAGE.value: TPTopoRest
}


class TPRestSession(object):
    def __init__(self, data_obj_type, address, api_key_env_var, properties_dict):
        self._data_obj_type = data_obj_type
        self._address = address
        self._api_key_env_var = api_key_env_var
        self._api_key = os.environ.get(api_key_env_var)
        self._properties_dict = properties_dict
        self._tp_rest_session = None

        logger.debug(
            f"self._data_obj_type = {self._data_obj_type}, self._address = {self._address},self._api_key_env_var = {self._api_key_env_var}, self._properties_dict = {self._properties_dict}, self._tp_rest_session = {self._tp_rest_session}")

    def __enter__(self):
        logger.debug(
            f"self._data_obj_type = {self._data_obj_type}, self._address = {self._address}")
        return self

    def __exit__(self, *args):
        logger.debug(f"exiting...")
        pass

    def list(self):
        res = self._tp_rest_session.list()
        return res

    def get(self):
        res = self._tp_rest_session.get()
        return res

    def create(self):
        res = self._tp_rest_session.create()
        return res

    def ingest(self, payload):

        res = self._tp_rest_session.ingest(payload)
        return res

    def delete(self):

        res = self._tp_rest_session.delete()
        return res

    def session(self):
        logger.debug(
            f"creating _tp_rest_session, self._data_obj_type = {self._data_obj_type}, self._address = {self._address}, self._properties_dict = {self._properties_dict}, self._tp_rest_session = {self._tp_rest_session}")
        self._tp_rest_session = TPRestClass[self._data_obj_type.value](
            self._address, self._api_key, self._properties_dict)
        return self


class TPNativeResponse(object):
    def __init__(self, result, error):
        self._result = result
        self._error = error

    @property
    def result(self):
        return self._result

    @property
    def error(self):
        return self._error


class TPNative(TPRestBase):  # inherite form TPRestBase but customize get method to be based on native protocol, customize view create to be based on native protocol for view
    # properties mapping to post body
    def __init__(self, data_obj_type, host, native_port, native_rest_port, database='default', **properties_dict):
        self._data_obj_type = data_obj_type
        self._host = host
        self._native_port = native_port
        self._native_rest_port = native_rest_port
        self._properties_dict = properties_dict
        self._database = database
        if TPNATIVE_DRIVER in sys.modules:  # if TPNATIVE_DRIVER is imported then init self._client
            self._client = Client(host, port=native_port)
        else:
            self._client = None
        self._home_uri = f"http://{self._host}:{self._native_rest_port}/proton/v1"
        self._uri = self._home_uri + "/ddl" + ResourcePath[data_obj_type.value]
        super().__init__(self._uri, {}, **self._properties_dict)

    def ingest(self, payload):  # only valid for stream, override TPRestBase for the uri
        try:
            name = self._properties_dict.get("name")
            self._ingest_uri = self._home_uri + "/ingest" + \
                ResourcePath[self._data_obj_type.value] + f"/{name}"
            logger.debug(
                f"self._ingest_uri = {self._ingest_uri}, ingest starts...")
            res = self.post(payload, self._ingest_uri)
            return res
        except (BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error

    def create(self):  # properties mapping to REST post json body, create stream/view
        self._response = None
        try:

            if self._data_obj_type == DataObjType.STREAM:  # for stream, use native REST protocol
                # remind: in neutron API, rest body has ttl field which should be ttl_expression in proton rest body, could do transfer here automatically

                if "ttl" in self._properties_dict and self._properties_dict["ttl"] != "":
                    self._properties_dict["ttl_expression"] = self._properties_dict["ttl"]
                    del self._properties_dict["ttl"]

                if "mode" in self._properties_dict and self._properties_dict["mode"] == "":
                    del self._properties_dict["mode"]

                # in neutron API, rest body has columns filed, in columns each item has a default field, which should be removed if it's ""
                if "columns" in self._properties_dict:
                    columns = self._properties_dict["columns"]
                    for column in columns:
                        if "default" in column and column["default"] == "":
                            del column["default"]

                self._response = super().create()
            elif self._data_obj_type == DataObjType.UDF:
                self._udf_uri = self._home_uri + \
                    ResourcePath[self._data_obj_type.value]
                self._response = super().create(self._udf_uri)
            elif self._data_obj_type == DataObjType.VIEW:  # for view, no native REST support, use native protocol
                query = self._properties_dict.get('query')
                materialized = self._properties_dict.get("materialized")
                view_name = self._properties_dict.get("name")
                if materialized == True:
                    create_view_sql = "create materialized view " + view_name + " as " + query
                else:
                    create_view_sql = "create view " + view_name + " as " + query
                native_res = self._client.execute(create_view_sql)
                self._response = TPNativeResponse(native_res, None)
            return self._response
        except (Exception) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error

    def delete(self):  # drop stream/view
        self._response = None
        try:
            name = self._properties_dict.get("name")
            if self._data_obj_type == DataObjType.UDF:  # for UDF use native REST protocol

                self._delete_uri = self._home_uri + \
                    ResourcePath[self._data_obj_type.value] + f"/{name}"
                logger.debug(
                    f"self._delete_uri = {self._delete_uri}, delete starts...")
                self._response = super().delete(self._delete_uri)
                return self._response
            else:  # for stream/view, use native protocol
                if self._data_obj_type == DataObjType.STREAM:
                    drop_sql = f"drop stream if exists {name} "
                elif self._data_obj_type == DataObjType.VIEW:
                    drop_sql = f"drop view if exists {name} settings enable_dependency_check = false"
                native_res = self._client.execute(drop_sql)
                self._response = TPNativeResponse(native_res, None)
            return self._response
        except (Exception) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error

    def list(self):
        res = self.get()
        if isinstance(res, TPNativeResponse):
            self._response = res.result
        else:
            self._response = res
        return self._response

    # sselect name, engine, ... from system.tables where database = 'default' and name = 'name' and engine = type
    def get(self, data_obj_name=None):
        logger.debug(
            f"self._data_obj_type = {self._data_obj_type}, data_obj_name = {data_obj_name}, self._host = {self._host}, self._native_port = {self._native_port}")
        self._response = None
        get_sql = "select name, engine, database, dependencies_table from system.tables where database = 'default' and engine = '" + \
            f"{self._data_obj_type.value}'"
        try:
            if data_obj_name is not None:
                get_sql = get_sql + "and name = '" + f"{data_obj_name}'"
            res = self._client.execute(get_sql)
            self._response = TPNativeResponse(res, None)
            logger.debug(f"self._response = {self._response}")
        except (Exception) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return error
        return self._response


class TPNativeSession(object):
    def __init__(self, data_obj_type, host, port, rest_port,  **properties_dict):
        self._data_obj_type = data_obj_type
        self._host = host
        self._port = port
        self._rest_port = rest_port
        self._properties_dict = properties_dict
        self._tp_native_session = None

        logger.debug(
            f"self._data_obj_type = {self._data_obj_type}, self._host = {self._host},self._port = {self._port}, self._rest_port = {self._rest_port}, self._properties_dict = {self._properties_dict}")

    def __enter__(self):
        logger.debug(
            f"enter(): self._data_obj_type = {self._data_obj_type}, self._host = {self._host}, self._port = {self._port}, self._rest_port = {self._rest_port}")
        return self

    def __exit__(self, *args):
        logger.debug(f"exiting...")
        self._tp_native_session._client.disconnect()

    def list(self):
        res = self._tp_native_session.list()
        return res

    def get(self):
        res = self._tp_native_session.get()
        return res

    def create(self):
        res = self._tp_native_session.create()
        return res

    def delete(self):
        res = self._tp_native_session.delete()
        return res

    def ingest(self, payload):
        res = self._tp_native_session.ingest(payload)
        return res

    def delete(self):
        res = self._tp_native_session.delete()
        return res

    def session(self):
        logger.debug(
            f"session(): self._data_obj_type = {self._data_obj_type}, self._host = {self._host},self._port = {self._port}, self._properties_dict = {self._properties_dict}, self._tp_native_session = {self._tp_native_session}")
        # self._tp_native_session = TPNativeClass[self._data_obj_type.value](self._host, self._port, self._properties_dict)
        self._tp_native_session = TPNative(
            self._data_obj_type, self._host, self._port, self._rest_port, database='default', **self._properties_dict)
        return self


if __name__ == '__main__':

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)

    logger.setLevel(logging.DEBUG)

    api_key = os.environ.get("TIMEPLUS_API_KEY")
    local_address = 'http://localhost:8000'
    address = 'https://dev.timeplus.cloud/tp-demo'
    env = Environment().address(address).apikey(api_key)
    stream_list = Stream(env=env).list()
    # logger.debug(f"stream_list = {stream_list}")

    # stream_properties =     {
    #     "created_by": {
    #         "name": "Jove Zhong",
    #         "id": "google-oauth2|109517076194156682691"
    #     },
    #     "created_at": "2023-04-18 18:14:17",
    #     "last_updated_by": {
    #         "name": "Jove Zhong",
    #         "id": "google-oauth2|109517076194156682691"
    #     },
    #     "last_updated_at": "2023-04-18 18:14:17",
    #     "name": "lpn_list_copy",
    #     "engine": "Stream",
    #     "ttl": "to_datetime(_tp_time) + INTERVAL 7 DAY",
    #     "columns": [
    #         {
    #             "name": "lpn",
    #             "type": "string",
    #             "nullable": False,
    #             "default": ""
    #         },
    #         {
    #             "name": "_tp_time",
    #             "type": "datetime64(3, 'UTC')",
    #             "nullable": False,
    #             "default": "now64(3, 'UTC')"
    #         }
    #     ],
    #     "logstore_retention_bytes": 10737418240,
    #     "logstore_retention_ms": 604800000,
    #     "is_external": False,
    #     "description": ""
    # }

    stream_properties = {
        "created_by": {
            "name": "Jove Zhong",
            "id": "google-oauth2|109517076194156682691"
        },
        "created_at": "2022-11-11 01:42:17",
        "last_updated_by": {
            "name": "Jove Zhong",
            "id": "google-oauth2|109517076194156682691"
        },
        "last_updated_at": "2022-11-11 01:42:17",
        "name": "auth",
        "engine": "Stream",
        "ttl": "",
        "columns": [
            {
                "name": "Code",
                "type": "string",
                "nullable": False,
                "default": ""
            },
            {
                "name": "Event",
                "type": "string",
                "nullable": False,
                "default": ""
            },
            {
                "name": "Description",
                "type": "string",
                "nullable": False,
                "default": ""
            },
            {
                "name": "_tp_time",
                "type": "datetime64(3, 'UTC')",
                "nullable": False,
                "default": "now64(3, 'UTC')"
            },
            {
                "name": "_tp_index_time",
                "type": "datetime64(3, 'UTC')",
                "nullable": False,
                "default": ""
            }
        ],
        "logstore_retention_bytes": 0,
        "logstore_retention_ms": 0,
        "is_external": False,
        "description": ""
    }

    tp_local_stream = TPStreamRest(address, api_key, stream_properties)
    res = tp_local_stream.create()

    sql = 'select * from table(auth) limit 10'
    tp_sse_rest = TPSSEQueryRest(address, api_key, sql)
    tp_sse_rest.query()
    query_res = tp_sse_rest.query_result()

    logger.debug(f"query_res = {query_res}")
    ingest_payload = query_res

    # stream_properties = {
    #     "name":"auth"
    # }

    # tp_local_stream = TPStreamRest(local_address, api_key, stream_properties)
    ingest_res = tp_local_stream.ingest(ingest_payload)
    logger.debug(f"ingest_res = {ingest_res}")

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
    tp_view = TPViewRest(address, api_key, view_name, query)
    view_list_response = tp_view.list()

    logger.debug(f"view_response = {view_list_response}")
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
    logger.debug(f"udf_list_response = {udf_list_reponse}")

    udf_create_response = udf.create()
    logger.debug(f"udf_create_response = {udf_create_response}")

    # logger.debug(f"view_response_json = {view_response_json}")
    # for view in view_response:
    #     logger.debug(f'name = {view["name"]}')
    #     logger.debug(f'query = {view["query"]}')

    # view_create_response = tp_view.create()
    # logging.debug(f"view_create_response = {view_create_response}")

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
    #     logger.debug(edge)
    #     logger.debug(edge.to_dict().get("source"))
