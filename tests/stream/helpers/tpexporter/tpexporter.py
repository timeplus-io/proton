import os, sys, logging, datetime, time, uuid, traceback, json, requests
import multiprocessing as mp
from enum import Enum, unique
import swagger_client
from swagger_client.rest import ApiException
from clickhouse_driver import Client
from tpclient import TPViewRest, TPUdfRest, TPStreamRest, TPSourceRest, TPSinkRest, TPTopoRest, TPSSEQueryRest, TPRestSession, DataObjType, TPProtocol, TPNative, TPNativeSession, ProtocolType, NativeProtocol, TPNativeResponse

import timeplus
#from timeplus import Environment, Stream


logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

@unique
class WorkspaceType(Enum):
    FILE = "file"
    ADDRESS = "address"
    NATIVE = "native"


@unique
class WorkspacePrivilege(Enum):
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"
    WRITE_ONLY = "write_only"

@unique
class WorkspaceStatus(Enum):
    UNCONNECTED = 0
    CONNECTED = 1
    READABLE = 2
    UNREADABLE = 3
    WRITEABLE = 4
    UNWRITEABLE = 5

# @unique
# class DataObjType(Enum):
#     SOURCE = "source"
#     STREAM = "stream"
#     VIEW = "view"
#     SINK = "sink"
#     UDF = "udf"
#     LINEAGE = "lineage"

# TPRestClass = {
#    DataObjType.SOURCE.value: TPSourceRest,
#    DataObjType.STREAM.value: TPStreamRest,
#    DataObjType.VIEW.value: TPViewRest,
#    DataObjType.SINK.value: TPSinkRest,
#    DataObjType.UDF.value: TPUdfRest,
#    DataObjType.LINEAGE.value: TPTopoRest
# }

@unique
class OperateTrackingType(Enum):
    SCHEMA_EXPORT = "schema_export"
    DATA_EXPORT = "data_export"

@unique
class ClientType(Enum):
    TIMEPLUS_REST = "timeplus_rest"
    TIMEPLUS_NATIVE = "timeplus_native"

@unique
class DataObjState(Enum):
    INIT = "init"
    DEPENDENCY_DISCOVERING = "dependency_discovering"
    DEPENDENCY_DISCOVERED = "dependency_discovered"
    SCHEMA_EXPORTING = "schema_exporting"
    SCHEMA_EXPORT_SKIPPED = "schema_exporting_skipped"
    SCHEMA_EXPORTED = "schema_exported"
    SCHEMA_EXPORT_FAILED = "schema_export_failed"
    DATA_EXPORTING = "data_exporting"
    DATA_EXPORTED = "data_exported"
    DATA_EXPORT_FAILED = "data_export_falied"


@unique
class OperateState(Enum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    INIT = "init"
    DEPENDENCY_DISCOVERING = "dependency_discovering"
    DEPENDENCY_DISCOVERED = "dependency_discovered"
    DEPENDENCY_DISCOVER_FAILED = "dependency_discover_failed"    
    STREAM_SCHEMA_EXPORTING = "stream_schema_exporting"
    STREAM_SCHEMA_EXPORTED = "stream_schema_exported" #_EXPORTED on Operate level means the _export phase is done.
    STREAM_SCHEMA_EXPORT_FAILED = "stream_schema_export_failed"
    STREAM_DATA_EXPORTING = "stream_data_exporting"
    STREAM_DATA_EXPORTED = "stream_data_exported"
    STREAM_DATA_EXPORT_FAILED = "stream_data_export_falied"    
    VIEW_SCHEMA_EXPORTING = "view_schema_exporting"
    VIEW_SCHEMA_EXPORTED = "view_schema_exported"
    VIEW_SCHEMA_EXPORT_FAILED = "view_schema_export_failed"
    UDF_SCHEMA_EXPORTING = "udf_schema_exporting"
    UDF_SCHEMA_EXPORTED = "udf_schema_exported"
    UDF_SCHEMA_EXPORT_FAILED = "udf_schema_export_failed"
    DONE = "done"        


class TpExportConfig(object):
    def __init__(self, config_path):
        #check if config_path exists
        self._config_path = config_path
        self.dict = self.read_config(config_path)

    def read_config(self, config_path):
        with open(self._config_path) as json_file:
            json_file_dict = json.load(json_file, strict=False)
        return json_file_dict

    def __str__(self): 
        return f"{self.dict}"

class TPWorkSpace(object):
    def __init__(self, name, type, address, api_key_env_var, access_privilege = WorkspacePrivilege.READ_ONLY, status = WorkspaceStatus.UNCONNECTED, **properties): #init TWorkSpace obj from workspace spec in config.json
        #todo: all the member variables should not be directly accessable
        #todo: simply the inteface signature of __init__, only put a spec as __init__ parameter and create all the member variables based on the spec, and set default value if no such key found in properties
        self.name = name  
        self.type = type
        self.address = address
        self.api_key_env_var = api_key_env_var            
        self.properties = properties
        self._status = status
        self.access_privilege = access_privilege
        self.nodes_exported = []
        self._import_dependencies = []        

    @property
    def status(self):
        return self._status
    
    @status.setter
    def status(self, status):
        self._status = status

    @property
    def import_dependencies(self):
        return self._import_dependencies
    
    @import_dependencies.setter
    def dependency(self, dependency):
        self._import_dependency = dependency
    
    def append_import_dependencies(self, data_obj):
        self._import_dependencies.append(data_obj)
    

    @classmethod
    def create_from_spec(cls, spec):
        name = spec.get("name")
        type = spec.get("type")
        workspace_type = WorkspaceType(type)
        address = spec.get("address")
        api_key_env_var = spec.get("api_key_env_var")
        access_privilege = spec.get("access_privilege")
        read_protocol = spec.get("read_protocol")
        write_protocol = spec.get("write_protocol")
        if workspace_type == WorkspaceType.NATIVE:
            access_privilege = WorkspacePrivilege.WRITE_ONLY #if workspace_type is native, access_privilege = write_only
            read_protocol = TPProtocol.TPNative #read_protocol is only for aclive check
            write_protocol = TPProtocol.TPNative
        else:
            if workspace_type == WorkspaceType.FILE:
                read_protocol = TPProtocol.TPFile 
                write_protocol = TPProtocol.TPFile 
            elif workspace_type == WorkspaceType.ADDRESS:
                if read_protocol is not None:
                    read_protocol = TPProtocol(read_protocol)
                else:
                    read_protocol = TPProtocol.TPRest
                if write_protocol is not None:
                    write_protocol = TPProtocol(write_protocol)
                else:
                    write_protocol = TPProtocol.TPRest
            if access_privilege is not None:
                access_privilege = WorkspacePrivilege(access_privilege)
            else:
                access_privilege = WorkspacePrivilege.READ_ONLY #if workspace_type is address or file, default access_privilege = read_only            

        tp_native_host = spec.get("tp_native_host",'localhost')
        tp_native_port = spec.get("tp_native_port",'8463')
        tp_native_rest_port = spec.get("tp_native_rest_port",'3218')

        #todo: put all spec into workspace.properties in workspace.__ini__() when create workspace obj, therefore all spec reading work could be saved
        status = WorkspaceStatus.UNCONNECTED
        env = None
        udfs = None
        lineage = None
        tp_workspace_obj = cls(name, workspace_type, address, api_key_env_var, access_privilege, status)
        tp_workspace_obj.properties[ProtocolType.READ.value] = read_protocol #for workspace of native type, read_protocol is used for the alive_check only
        tp_workspace_obj.properties[ProtocolType.WRITE.value] = write_protocol
        tp_workspace_obj.properties[NativeProtocol.HOST.value] = tp_native_host
        tp_workspace_obj.properties[NativeProtocol.PORT.value] = tp_native_port
        tp_workspace_obj.properties[NativeProtocol.REST_PORT.value] = tp_native_rest_port

        if tp_workspace_obj.type in (WorkspaceType.ADDRESS, WorkspaceType.NATIVE):
            TPWorkSpace.alive_check(tp_workspace_obj)
            logger.debug(f"tp_workspace_obj.status = {tp_workspace_obj.status}")
            if tp_workspace_obj.status == WorkspaceStatus.CONNECTED and tp_workspace_obj.type == WorkspaceType.ADDRESS: #only support source from address type workspace so far
                for type in DataObjType:
                    resources_of_type = cls.resources(tp_workspace_obj, type)
                    tp_workspace_obj.properties[type.value] = resources_of_type
                logger.debug(f"tp_workspace_obj.properties.keys() = {tp_workspace_obj.properties.keys()}")
        elif tp_workspace_obj.type == WorkspaceType.FILE:
            workspace_dir_path = TPWorkSpace.alive_check(tp_workspace_obj)
            if workspace_dir_path:
                lineage_file_path = os.path.join(workspace_dir_path, "lineage.json")
                if os.path.exists(lineage_file_path):
                    with open(lineage_file_path, "r") as lineage_file:
                        lineage = json.load(lineage_file_path, strict=False)
                        tp_workspace_obj.lineage = lineage               
        logger.debug(f"tp_workspace_obj.name = {tp_workspace_obj.name}, tp_workspace_obj.type = {tp_workspace_obj.type}, tp_workspace_obj.status = {tp_workspace_obj.status}, tp_workspace_obj.access_privilege = {tp_workspace_obj.access_privilege}")
        return tp_workspace_obj  


    @classmethod
    def resources(cls, workspace, data_obj_type): #only support TPRest protocol
        resource_list = [] 
        if workspace.status == WorkspaceStatus.UNCONNECTED:
            logger.debug(f"workspace.name = {workspace.name}, workspace.status = {workspace.status}, data_obj_type = {data_obj_type}, get resources skipped.") 
            return resource_list
        else:
            logger.debug(f"workspace.name = {workspace.name}, workspace.status = {workspace.status}, data_obj_type = {data_obj_type}, get resources starts.")       
        
            try:
                if workspace.properties[ProtocolType.READ.value] == TPProtocol.TPRest: #only supports get resources from workspace by TPRest protocol
                    # api_key_env_var = workspace.api_key_env_var
                    # api_key = os.environ.get(api_key_env_var)            
                    # tp_resource = TPRestClass[data_obj_type.value](workspace.address, api_key, None)
                    with TPRestSession(data_obj_type, workspace.address, workspace.api_key_env_var, None).session() as tp_rest_session: 
                        resource_list = tp_rest_session.list()
                else:
                    pass #load lineage from file type workspace logic here
                logger.debug(f"workspace.name = {workspace.name}, workspace.status = {workspace.status}, resources of data_obj_type = {data_obj_type} retrevied, len(resource_list) = {len(resource_list)}.") 
                return resource_list
            except(BaseException) as error:
                logger.debug("Exception, error = {error}")
                traceback.print_exc()
                return resource_list
    


    @classmethod
    def alive_check(cls, workspace):
        logger.debug(f"alive_checking, workspace.name = {workspace.name}, workspace.address = {workspace.address}")
        try:
            if workspace.type in (WorkspaceType.ADDRESS, WorkspaceType.NATIVE):
                res = False
                # api_key_env_var = workspace.api_key_env_var
                # api_key = os.environ.get(api_key_env_var)
                 
                # tp_stream = TPStreamRest(workspace.address, api_key, None)
                # stream_list = tp_stream.list()
                #a = TPRestSession(DataObjType.STREAM, workspace.address, workspace.api_key_env_var, None).session()
                #logger.debug(f"a = {a}")
                read_protocol = workspace.properties.get(ProtocolType.READ.value)
                stream_list = ''
                if read_protocol == TPProtocol.TPRest:
                    with TPRestSession(DataObjType.STREAM, workspace.address, workspace.api_key_env_var, None).session() as tp_stream_session:
                        stream_list = tp_stream_session.list()
                        #logger.debug(f"len(stream_list) = {len(stream_list)}")
                else: # if read_protocol is TPNative, 
                    with TPNativeSession(DataObjType.STREAM, workspace.properties[NativeProtocol.HOST.value], workspace.properties[NativeProtocol.PORT.value], workspace.properties[NativeProtocol.REST_PORT.value]).session() as tp_stream_session:
                        stream_list = tp_stream_session.list()                    
                    pass
                if isinstance(stream_list, list):
                    workspace.status = WorkspaceStatus.CONNECTED
                    res = True
                else:
                    workspace.status = WorkspaceStatus.UNCONNECTED
                    res = False
                return res
            else:
                if os.path.exists(workspace.address):
                    return workspace.address
                else:
                    return False                
        except(BaseException) as error:
            logger.debug(f"workspace connection failed, error = {error}")                
            traceback.print_exc()
            return error


    def __str__(self): 
        return f"name = {self.name}, type = {self.type}, address = {self.address}, api_key_env_var = {self.api_key_env_var}"


class TPExportStreamRule(object):
    def __init__(self, stream, rule):
        self.stream = stream
        self.rule = rule
    
    @classmethod
    def create_from_spec(cls, spec):
        stream_name = spec.get("name")
        stream_rule = spec.get("rule")
        tp_export_stream_rule_obj = cls(stream_name, stream_rule)
        return tp_export_stream_rule_obj
    
    def __str__(self): 
        return f"name = {self.stream}, rule = {self.rule}"    


class TPExportStremRules(object):
    def __init__(self, stream_rules):
        self.stream_rules = stream_rules

    def __str__(self): 
        return f"stream_rules = {self.stream_rules}"  
    
    @classmethod
    def create_from_spec(cls, spec):
        tp_stream_rules = []
        if spec is None:
            tp_stream_rules_obj = None
        else:
            for rule_item_spec in spec:
                tp_export_rule_item = TPExportStreamRule.create_from_spec(rule_item_spec)
                tp_stream_rules.append(tp_export_rule_item)
        tp_stream_rules_obj = cls(tp_stream_rules)
        return tp_stream_rules_obj


class TPExportPolicy(object):
    def __init__(self, stream_rules):
        self.policy = stream_rules
    @classmethod
    def create_from_spec(cls, spec):
        if spec is not None:
            stream_rules_spec = spec.get('streams')
            if stream_rules_spec is not None:
                stream_rules_obj = TPExportStremRules.create_from_spec(stream_rules_spec)
            else:
                stream_rules_obj = None
            tp_export_policy_obj = cls(stream_rules_obj)
        else:
            tp_export_policy_obj = None
        return tp_export_policy_obj

    @classmethod
    def get_stream_rules_by_name(cls, tp_export_policy_obj, name):
        stream_rules = tp_export_policy_obj.stream_rules
        for rule in stream_rules:
            stream_name_in_rule = rule.get("stream")
            if stream_name_in_rule == name:
                return rule
        return None



class TPDataObj(object):
    def __init__(self, id, name, type,  properties,  state, workspace, dependency_discovery, data_exporting_policy, depends_stream_data_volume = None, dependencies = None):
        self.id = id
        self.name = name
        self.state = state
        self.type = type
        self.properties = properties #a super set dict of REST body of the data_obj post, will be used call the post REST api
        self.workspace = workspace
        self.dependency_discovery = dependency_discovery
        self.data_exporting_policy = data_exporting_policy
        self.dependency_stream_data_volume = depends_stream_data_volume
        self.dependencies = []
        self.schema_export_tracking = {}
        self.data_export_tracking = {}
    
    @classmethod
    def create_from_spec(cls, data_obj_spec, workspace, data_exporting_policy): #data_obj_sepc is what defined in config.json,
        # logger.debug(f"data_obj_spec = {data_obj_spec}, workspace.name = {workspace.name}, workspace.status = {workspace.status}")
        id = data_obj_spec.get("id")
        name = data_obj_spec.get("name")
        type = DataObjType(data_obj_spec.get("type"))


        #if TPWorkSpace.alive_check(workspace):
        if workspace.status != WorkspaceStatus.UNCONNECTED:
            if workspace.type == WorkspaceType.ADDRESS:
                data_obj_properties = cls.get_properties_from_workspace(name,type ,workspace)
                id = data_obj_properties.get("id")            
                state = DataObjState.INIT
                dependency_discovery = data_obj_spec.get("dependency_discovery")
                dependency_stream_data_volume = data_obj_spec.get("depends_stream_data_volume")
                tp_data_obj = cls(id, name, type, data_obj_properties, state, workspace, dependency_discovery,data_exporting_policy,dependency_stream_data_volume)
                logger.debug(f"tp_data_obj.name = {tp_data_obj.name}, tp_data_obj.type = {tp_data_obj.type}, tp_data_obj.properties = {tp_data_obj.properties}")
                return tp_data_obj
            else: # todo: data_obj creating based on file based workspace logic
                pass
        else:
            logger.debug(f"workspace.name = {workspace.name}, workspace.type = {workspace.type}, workspace.status = {workspace.status}, not alive, data_obj.name = {name} could not be created.")
            return None  
    
    @classmethod
    def create_from_properties(cls, data_obj_peroperties, workspace, data_exporting_policy, data_obj_type = None): #data_obj_peroperties could be REST body of a udf data_obj, could be defined_obj_spec read from config.json
        try:
            if data_obj_peroperties is not None:
                data_obj_id = data_obj_peroperties.get("id")
                data_obj_name = data_obj_peroperties.get("name")
                if data_obj_type is None: #when data_obj_type is None, get data_obj_type from properties, for udf,type in properties are about type of UDF but not data_obj_type.value, 
                    type = data_obj_peroperties.get("type")
                    data_obj_type = DataObjType(type)
                if data_obj_id is None:
                    data_obj_id = data_obj_name
                data_obj_properties = data_obj_peroperties
                data_obj_state = DataObjState.INIT
                dependency_discovery = data_obj_peroperties.get("dependency_discovery")
                if dependency_discovery is None:
                    data_obj_dependency_discovery = "no"
                dependency_stream_data_volume = data_obj_peroperties.get("depends_stream_data_volume")
                source_data_obj = TPDataObj(data_obj_id, data_obj_name, data_obj_type, data_obj_properties, data_obj_state, workspace, data_obj_dependency_discovery, data_exporting_policy, dependency_stream_data_volume)
                return source_data_obj
        except(BaseException) as error:
            logger.debug(f"error = {error}")
            traceback.print_exc()
            return error
    
    # @classmethod
    # def create_data_obj_from_udf(cls, udf, workspace, data_exporting_policy):
    #     try:
    #         if udf is not None:
    #             udf_id = udf.get("id")
    #             udf_name = udf.get("name")
    #             if udf_id is None:
    #                 udf_id = udf_name
    #             udf_properties = udf
    #             type = DataObjType.UDF
    #             state = DataObjState.INIT
    #             dependency_discovery = "no"
    #             source_data_obj = TPDataObj(udf_id, udf_name,type, udf_properties, state, workspace, dependency_discovery, data_exporting_policy, depends_stream_data_volume = None)
    #             return source_data_obj
    #     except(BaseException) as error:
    #         logger.debug(f"error = {error}")
    #         traceback.print_exc()
    #         return error    
    
    @classmethod
    def create_from_node(cls, node, workspace, data_exporting_policy): #create data_obj from lineage node
        try:
            if node is not None:
                data_obj_id = node.get("id")
                data_obj_name = node.get("name")
                node_type = node.get("type")
                data_obj_type = DataObjType(node_type)
                state = DataObjState.INIT
                data_obj_properties = {}
                logger.debug(f"data_obj_id = {data_obj_id}, data_obj_name = {data_obj_name}, data_obj_type = {data_obj_type}, workspace.name = {workspace.name}, workspace.properties.keys() = {data_obj_properties.keys()}")
                if workspace.properties is not None:  
                    data_obj_properties = cls.get_properties_from_workspace(data_obj_name, data_obj_type, workspace)
                    # workspace_properties_by_type = workspace.properties.get(data_obj_type.value)
                    # for item in workspace_properties_by_type:
                    #     item_name = item.get("name")
                    #     item_type = item.get("type")
                    #     if item_name == data_obj_name:
                    #         data_obj_properties = item
                else:
                    data_obj_properties = node.get("properties")
                
                dependency_discovery = "auto"
                source_data_obj = TPDataObj(data_obj_id, data_obj_name, data_obj_type, data_obj_properties, state, workspace, dependency_discovery, data_exporting_policy, depends_stream_data_volume = None)
        except(BaseException) as error:
            logger.debug(f"error = {error}")
            traceback.print_exc()
        return source_data_obj
    
    @classmethod
    def get_properties_from_workspace(cls, data_obj_name, data_obj_type, workspace): #type and name decides a unique data_obj and id could be retreieved by (name, type)
        logger.debug(f"data_obj_name = {data_obj_name}, data_obj_type = {data_obj_type}, workspace.name = {workspace.name}, workspace.status = {workspace.status}")
        data_obj_properties = {}
        workspace_properties_by_type = workspace.properties.get(data_obj_type.value)
        for workspace_property in workspace_properties_by_type:
            if workspace_property["name"] == data_obj_name:
                data_obj_properties = workspace_property
        return data_obj_properties

    
    

    def add_dependency(self, depedency_data_obj):
        self.dependencies.append(depedency_data_obj)
    
    def set_dependency(self, dependencies_list):
        self.dependencies = dependencies_list

    def read_lineage(self): #read linage from workspace via rest API
        print()
    
    def dependency(self, data_obj):
        print()
        depend_graph = []
        return depend_graph

    def _dependency_export(self):
        print()
    

    def _get_data(self, source_workspace, target_workspace):
        logger.debug(f"getting data of data_obj.name = {self.name}, source_workspace_.address = {source_workspace.address}, target_workspace.address = {target_workspace.address}")

    def exports_data(self, target_workspaces): #todo: logging
                
        print(f"getting data of data_obj.name = {self.name}, source_workspace.address = {self.workspace.address}, target_workspaces = {target_workspaces}, data exporting.")
        #get data from source_work_space
        data_volume = 0
        source_api_key_env_var = self.workspace.api_key_env_var
        source_api_key = os.environ.get(source_api_key_env_var)
        snap_shot_volumn = 1000 #hardcode 1000 rows for now, todo: get snap_shot_volumn from config
        engine = self.properties.get("engine")
        try:
            if self.state == DataObjState.SCHEMA_EXPORTED:
                if engine is not None and engine == "ExternalStream":
                    print(f"data_obj_name = {self.name}, data_obj_type = {self.type}, engine = {engine}, ingestion skipped.")
                else:
                    sql = f"select * from table(" + self.name + ") limit " + str(snap_shot_volumn)        
                    tp_sse_rest = TPSSEQueryRest(self.workspace.address, source_api_key, sql)
                    tp_sse_rest.query()
                    query_res = tp_sse_rest.query_result()
                    data = query_res["data"]
                    print(f"data_obj_name = {self.name}, data_obj_type = {self.type}, engine = {engine}")
                    data_volume = len(data)
                    #print(f"getting data of data_obj.name = {self.name}, source_workspace.address = {self.workspace.address}, data_volume = {data_volume}, retreived.")
                    ingest_payload = query_res
                    #print(f"ingest_payload = {ingest_payload}")
                
                    if data_volume > 0:
                        for target_workspace in target_workspaces: #going through all target_workspaces with exception cathcing
                            try: 
                                access_privilege = target_workspace.access_privilege
                                if access_privilege == WorkspacePrivilege.READ_WRITE or access_privilege == WorkspacePrivilege.WRITE_ONLY:
                                    #if target_workspace.type == WorkspaceType.ADDRESS:
                                    if target_workspace.properties[ProtocolType.WRITE.value] == TPProtocol.TPRest or target_workspace.properties[ProtocolType.WRITE.value] == TPProtocol.TPNative:
                                        if target_workspace.properties[ProtocolType.WRITE.value] == TPProtocol.TPRest:
                                            target_api_key_env_var = target_workspace.api_key_env_var
                                            target_api_key = os.environ.get(target_api_key_env_var)
                                            #tp_target_stream = TPStreamRest(target_workspace.address, target_api_key, self.properties)
                                            ingest_res = ''
                                            with TPRestSession(DataObjType.STREAM, target_workspace.address, target_workspace.api_key_env_var, self.properties).session() as tp_target_stream:
                                                ingest_res = tp_target_stream.ingest(ingest_payload)
                                            
                                        elif target_workspace.properties[ProtocolType.WRITE.value] == TPProtocol.TPNative:
                                            target_workspace_host = target_workspace.properties[NativeProtocol.HOST.value]
                                            target_workspace_port = target_workspace.properties[NativeProtocol.PORT.value]
                                            target_workspace_rest_port = target_workspace.properties[NativeProtocol.REST_PORT.value]                                             
                                            with TPNativeSession(DataObjType.STREAM, target_workspace_host, target_workspace_port,target_workspace_rest_port, **self.properties).session() as tp_target_stream_native:
                                                ingest_res = tp_target_stream_native.ingest(ingest_payload)
                                        if isinstance(ingest_res, requests.Response):  
                                            if ingest_res.status_code != 200:
                                                #print(f"data_obj_name = {self.name}, data_obj_type = {self.type}, data = {data}")  
                                                print(f"data_obj_name = {self.name}, data_obj_type = {self.type}, target_workspace.name = {target_workspace.name}, ingest_res = {ingest_res}, ingest_res_json = {ingest_res.text}")
                                            else:
                                                print(f"data_obj_name = {self.name}, data_obj_type = {self.type},target_workspace.name = {target_workspace.name}, data_volume = {data_volume},ingest_res = {ingest_res}, ingestion done.")
                                        else:
                                            print(f"data_obj_name = {self.name}, data_obj_type = {self.type},target_workspace.name = {target_workspace.name}, data_volume = {data_volume},ingest_res = {ingest_res}, ingestion failed, \n ingestion error = {ingest_res}.")                                
                                    
                                    else:
                                        print() #todo: export data to file based workspace logic here
                                        raise Exception(f"File protocol based exporting now supported.")
                                print(f"data_obj_name = {self.name}, data_obj_type = {self.type}, target_workspace.name = {target_workspace.name}, target_workspace.access_privilege ={target_workspace.access_privilege}, data write is now allowed and skipped.")
                            except(BaseException) as error:
                                logger.debug(f"Exception, error = {error}")
                                traceback.print_exc()
                    else:
                        print(f"data_obj_name = {self.name}, data_obj_type = {self.type}, ingestion skipped.")
            else:
                raise Exception(f"data_obj_name = {self.name}, data_obj_type = {self.type}, data_obj_state = {self.state}, schema not exported, ingestion skipped.")
            return
        except(BaseException) as error:
            logger.debug(f"Exception, error = error")
            traceback.print_exc()
            return error


    def _export_view_schema_native(self, view_name, properties, target_workspace):
        try:
            logger.debug(f"data_obj_name = {self.name}, view_name = {view_name}, properties = {properties}")
            is_materialized = properties.get("is_materialized")
            query = properties.get("query")
            if is_materialized == True:
                create_view_sql =  "create materialized view if not exists " + view_name + " as " + query
            else:
                create_view_sql =  "create view if not exists " + view_name + " as " + query

            #todo: replace proton python client code with Gluon view api code
            pyclient = Client("localhost", port=8463)
            logger.debug(f"create_view_sql = {create_view_sql}")
            res = pyclient.execute(create_view_sql)
            logger.debug(f"data_obj_name = {self.name}, view_name = {view_name}, properties = {properties}, res = {res}, schema exported")
        except(BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()


    def _export_schema_2_site(self, target_workspace): #only view, stream, udfs so far
        try:
            if target_workspace.type in (WorkspaceType.ADDRESS, WorkspaceType.NATIVE): #only handling exporting to workspace with type of address and native
                target_workspace_host = target_workspace.properties[NativeProtocol.HOST.value]
                target_workspace_port = target_workspace.properties[NativeProtocol.PORT.value]
                target_workspace_rest_port = target_workspace.properties[NativeProtocol.REST_PORT.value]                
                if self.type == DataObjType.VIEW or self.type == DataObjType.STREAM or self.type == DataObjType.UDF:
                    logger.debug(f"data_obj_name = {self.name}, target_workspace.name = {target_workspace.name}, target_workspace.type = {target_workspace.type}, target_workspace.address = {target_workspace.address}, target_workspace_host = {target_workspace_host}, target_workspace_port = {target_workspace_port}, target_workspace_rest_port = {target_workspace_rest_port}")
                    # target_api_key_env_var = target_workspace.api_key_env_var
                    # target_api_key = os.environ.get(target_api_key_env_var)
                    # tp_obj_rest = TPRestClass[self.type.value](target_workspace.address,target_api_key,self.properties)
                    res = ''
                    if target_workspace.properties[ProtocolType.WRITE.value] == TPProtocol.TPRest: #based on the write_protocol property to decide TPRestSession or TPNativeSession is used for exporting
                        with TPRestSession(self.type, target_workspace.address, target_workspace.api_key_env_var,self.properties).session() as tp_rest_session:
                            res = tp_rest_session.create()
                            logger.debug(f"res of tp_rest_session.create() = {res}")
                    elif target_workspace.properties[ProtocolType.WRITE.value] == TPProtocol.TPNative:         
                        with TPNativeSession(self.type, target_workspace_host, target_workspace_port, target_workspace_rest_port, **self.properties).session() as tp_native_session:
                            res = tp_native_session.create()                        
                            logger.debug(f"res of tp_native_session.create() = {res}")
                    if isinstance(res, requests.Response) or isinstance(res, TPNativeResponse):
                        if isinstance(res, requests.Response):
                            logger.debug(f"data_obj_name = {self.name}, data_obj_type = {self.type}, properties = {self.properties}, data_obj schema export res = {res}, res.message = {res.json()}")
                            if  res.status_code == 200 or  res.status_code == 201:
                                self.schema_export_tracking[target_workspace.name]["state"] = DataObjState.SCHEMA_EXPORTED.value #use value here for JSON serializable
                                self.schema_export_tracking[target_workspace.name]["response"] = res.json()
                                target_workspace.append_import_dependencies(self)                           
                            else:
                                self.schema_export_tracking[target_workspace.name]["state"] = DataObjState.SCHEMA_EXPORT_FAILED.value #use value here for JSON serializable
                                self.schema_export_tracking[target_workspace.name]["response"] = res.json()
                            logger.debug(f'self.schema_export_tracking[{target_workspace.name}]["state"] = {self.schema_export_tracking[target_workspace.name]["state"]}')
                        else:
                            logger.debug(f"data_obj_name = {self.name}, data_obj_type = {self.type}, properties = {self.properties}, data_obj schema export res.result = {res.result}") 
                            self.schema_export_tracking[target_workspace.name]["state"] = DataObjState.SCHEMA_EXPORTED.value #use value here for JSON serializable
                            self.schema_export_tracking[target_workspace.name]["response"] = res.result
                            target_workspace.append_import_dependencies(self)                                                                 
                    else: #if res is not a requests.Response, it's a error caught
                        logger.debug(f"data_obj_name = {self.name}, data_obj_type = {self.type}, properties = {self.properties}, data_obj schema export res = {res}")
                        self.schema_export_tracking[target_workspace.name]["state"] = DataObjState.SCHEMA_EXPORT_FAILED.value #use value here for JSON serializable
                        self.schema_export_tracking[target_workspace.name]["response"] = f"{res}"
                        logger.debug(f'self.schema_export_tracking[{target_workspace.name}]["state"] = {self.schema_export_tracking[target_workspace.name]["state"]}')
                    return res
                else:
                    logger.info(f"data_obj_name = {self.name}, data_obj_type = {self.type}, skipped")
                    self.schema_export_tracking[target_workspace.name]["state"] = DataObjState.SCHEMA_EXPORT_SKIPPED.value
                    self.schema_export_tracking[target_workspace.name]["response"] = f"data_obj_name = {self.name}, data_obj_type = {self.type}, skipped"
            else:
                logger.info(f"data_obj_name = {self.name}, data_obj_type = {self.type}, target_workspace.type = {target_workspace.type}, skipped")
                self.schema_export_tracking[target_workspace.name]["state"] = DataObjState.SCHEMA_EXPORT_SKIPPED.value
                self.schema_export_tracking[target_workspace.name]["response"] = f"data_obj_name = {self.name}, data_obj_type = {self.type}, target_workspace.type = {target_workspace.type},not support file protocol based exporting now, skipped"                
                pass #export to file based workspace logic
                raise Exception(f"data_obj_name = {self.name}, data_obj_type = {self.type},target_workspace.type = {target_workspace.type}, not support file protocol based exporting now, skipped")             
        except(BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            self.state = DataObjState.SCHEMA_EXPORT_FAILED
            return error        



    # def  _export_schema_2_site(self, target_workspace):
    #     res = None
    #     try:
    #         if target_workspace.properties[ProtocolType.WRITE.valute] in (TPProtocol.TPRest, TPProtocol.TPNative):
    #             res = self._export_data_obj_schema_rest(target_workspace)
    #         else: # export to file based workspace logic here
    #             pass
    #         return res  
    #     except(BaseException) as error:
    #         logger.debug(f"Exception, error = {error}")
    #         traceback.print_exc()
    #         return error         

    # def _export_schema_2_site(self, target_workspace):
    #     if target_workspace.type == WorkspaceType.ADDRESS:    
    #         res = self._export_data_obj_schema(target_workspace)
    #         return res
    #     elif target_workspace.type == WorkspaceType.FILE:
    #         pass # todo: exporting to file based site logic
    #     logger.debug("res = {res}")



    def _export_schema_2_file(self, target_workspace):
        logger.debug(f"data_obj_name = {self.name}, target_workspace.name = {target_workspace.name}, target_workspace.type = {target_workspace.type}, target_workspace.address = {target_workspace.address}") 
        pass #logic handing file protocol exporting here       

    def exports_schema(self, target_workspaces):
        try:
            
            if self.state == DataObjState.SCHEMA_EXPORTED or self.state == DataObjState.SCHEMA_EXPORTING or self.state == DataObjState.SCHEMA_EXPORT_FAILED:
                logger.debug(f"self.name = {self.name}, self.state = {self.state}, already in exporting or exported, skipped")
                return

            #logger.info(f"source_data_obj_name = {self.name}, source_data_obj_type = {self.type}, source_workspace = {self.workspace.name}, target_workspaces = {target_workspaces}, source_data_obj.dependencies = {self.dependencies}, exporting starts")
            self.state = DataObjState.SCHEMA_EXPORTING
            if self.dependencies is not None:
                for dependency in reversed(self.dependencies):
                    if isinstance(dependency,TPDataObj): #dependency is a TPDataObj
                        logger.debug(f"data_obj.name = {self.name}, dependency = {dependency.name}, dependency_type = {dependency.type}, dependency exporting starts")
                        dependency.exports_schema(target_workspaces)


            for workspace in target_workspaces:
                logger.info(f"self.name = {self.name}, self.type = {self.type}, workspace.name = {workspace.name}, workspace.type = {workspace.type}, workspace.address = {workspace.address}, workspace.access_privilege = {workspace.access_privilege}")
                self.schema_export_tracking[workspace.name] = {} #when iterating target_workspaces for exporting in exports_schema(), set self.schema_export_tracking based on target_workspace.name
                access_privilege = workspace.access_privilege
                if access_privilege == WorkspacePrivilege.READ_ONLY:
                    logger.info(f"workspace.name = {workspace.name}, workspace.type = {workspace.type}, workspace.address = {workspace.address}, workspace.access_privilege = {workspace.access_privilege}, schema write is not allowed, skip")
                    self.schema_export_tracking[workspace.name]["state"] = DataObjState.SCHEMA_EXPORT_SKIPPED.value #use value here for JSON serializable
                    self.schema_export_tracking[workspace.name]["response"] = 'skipped'                    
                elif access_privilege == WorkspacePrivilege.READ_WRITE or access_privilege == WorkspacePrivilege.WRITE_ONLY:
                    logger.info(f"source_data_obj_name = {self.name}, source_data_obj_type = {self.type}, source_workspace = {self.workspace.name}, target_workspace.name = {workspace.name}, target_workspace.type = {workspace.type}, source_data_obj.dependencies = {self.dependencies}, exporting starts")
                    if workspace.type == WorkspaceType.FILE:
                        self._export_schema_2_file(workspace)
                    elif workspace.type == WorkspaceType.ADDRESS or workspace.type == WorkspaceType.NATIVE:
                        self._export_schema_2_site(workspace)

            self.state = DataObjState.SCHEMA_EXPORTED #self.state of the DataObj, SCHEMA_EXPROTED means the exporting action is done, for the detailed success or fail of schema exporting, check self.schema_export_tracking
            logger.info(f"self.name = {self.name}, self.state = {self.state} exports done.")
            
        except(BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exec()

 
    def imports(self, import_dir = './'):
        print()

    def _replica(self, target_workspace_env):
        print()
    
    def sources(self): #return sources of the data obj
        print()
    
    def target(self): #return target of the data obj
        print()

    def __str__(self): 
        return f"id = {self.id}, name = {self.name}, type = {self.type}, workspace = {self.workspace.name}, dependency_discovery = {self.dependency_discovery},data_exporting_policy = {self.data_exporting_policy},dependency_stream_data_volume = {self.dependency_stream_data_volume}"



class TPExportOperate(object):
    def __init__(self, name, mode, source_workspace, state, source_data_objs, data_exporting_policy, target_workspaces, spec):
        self.name = name
        if mode is None:
            mode = "snapshot" #if no mode in spec, set mode to snapshot, todo: refine        
        self.mode = mode
        self.source_workspace = source_workspace
        self.state = state
        self.source_data_objs = source_data_objs
        self.data_exporting_policy = data_exporting_policy
        self.target_workspaces = target_workspaces
        self.spec = spec
        self.tracking = {}      


    
    @classmethod
    def create_defined_source_data_objs(cls, spec, source_workspace, data_exporting_policy):
        defined_objs = []
        try:
            logger.debug(f"creating defined source_data_objs...")
            defined_data_objs_spec = spec.get("defined_data_objs")
            if defined_data_objs_spec is not None and isinstance(defined_data_objs_spec, list):
                for data_obj_spec in defined_data_objs_spec:
                    defined_obj = TPDataObj.create_from_properties(data_obj_spec, source_workspace, data_exporting_policy)
                    defined_objs.append(defined_obj)
            logger.debug(f"defined_data_objs created = {defined_objs}")
            return defined_objs
        except(BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return defined_objs                
        
        
    
    
    @classmethod
    def create_udf_source_data_objs(cls, source_workspace, data_exporting_policy):
        udf_objs = []
        try:
            logger.debug(f"creating udf source_data_objs, source_workspace.name = {source_workspace.name}...")
            udfs = source_workspace.properties.get("udfs")
            if udfs is not None:
                for udf in udfs:
                    #udf_data_obj = TPDataObj.create_data_obj_from_udf(udf, source_workspace, data_exporting_policy)
                    udf_data_obj = TPDataObj.create_from_properties(udf, source_workspace, data_exporting_policy, DataObjType.UDF) #udf properties is a spec too, but due to type in UDF properties is not DataType, so set the data_obj_typ seperatly
                    udf_objs.append(udf_data_obj)
            return udf_objs
        except(BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return udf_objs        

    @classmethod
    def _data_objs_stats(cls, data_obj_list):
        stats = {}
        for item in DataObjType:
            stats[item.value] = {}

        for item in DataObjType:
            stats[item.value]["count"] = 0
            stats[item.value]["names"] = []

        logger.debug(f"stats inited, stats = {stats}")
        if data_obj_list is not None:
            for item in data_obj_list:
                if item is not None:
                    logger.debug(f"item.name = {item.name}, item.type = {item.type}")                    
                    stats[item.type.value]["count"] = stats[item.type.value]["count"] + 1
                    stats[item.type.value]["names"].append(item.name)
        return stats
            

    @classmethod
    def create_source_data_objs(cls, operate_spec, source_workspace, data_exporting_policy): #create source_data_objs
        logger.debug(f"spec = {operate_spec}, source_workspace.name = {source_workspace.name}, source_workspace.properties[{ProtocolType.READ.value}] = {source_workspace.properties[ProtocolType.READ.value]}, data_exporting_policy = {data_exporting_policy}")           
        source_data_objs_spec = operate_spec.get("source_data_objs")
        status = operate_spec.get("status")
        source_data_objs = []
        operate_name = operate_spec.get("name")
        if source_workspace.properties[ProtocolType.READ.value] == TPProtocol.TPRest: #only suppports create source_data_objs from workspace with TPRest read protocol
            if not isinstance(source_data_objs_spec,list) and source_data_objs_spec == 'All' and status == "enabled": #compose the data_obj spec by going through the lineage and create data_obj objects and put into soruce_data_objs when the source_data_objs = 'All' but not a list
                source_data_objs_discoverd = cls.create_source_data_objs_from_workspace(source_workspace, data_exporting_policy)
                source_data_objs.extend(source_data_objs_discoverd)
            elif isinstance(source_data_objs_spec,list):
                logger.debug(f"specifed source_data_objs_spec = {source_data_objs_spec}")
                #due to udf_objs are not in lineage, so udfs data_objs are created seperately. 
                udf_objs = cls.create_udf_source_data_objs(source_workspace, data_exporting_policy) #create udf data_objs by going through udfs gotten from source_workspace via REST and add into source_data_objs list
                source_data_objs.extend(udf_objs)                                    
                for source_data_obj_spec in source_data_objs_spec: # process the source_data_objs is a list 
                    #source_data_obj_spec[data_exporting_policy] = data_exporting_policy # transer the data_exporting_policy of the operate to the data_obj
                    source_data_obj = TPDataObj.create_from_spec(source_data_obj_spec, source_workspace, data_exporting_policy) #create source_data_obj from defination in config.json
                    source_data_objs.append(source_data_obj) 
            #create 'defined_data_objs' by reading spec, and add into source_data_objs list
            defined_objs = cls.create_defined_source_data_objs(operate_spec, source_workspace, data_exporting_policy)
            logger.debug(f"operate_name = {operate_name}, defined_objs created = {defined_objs}")
            source_data_objs.extend(defined_objs)
            #todo: if none of above conditions meet, errors
            source_data_objs_stats = cls._data_objs_stats(source_data_objs)
            logger.debug(f"operate_name = {operate_name}, source_data_objs created stats = {source_data_objs_stats}")
        else: # for read_protocol other than TPRest, support later
            logger.debug(f"source_workspace.properties[{ProtocolType.READ.value}] = {source_workspace.properties[ProtocolType.READ.value]}, source_workspace read protocol is not supported yet")
            pass

        return source_data_objs
    
    
    @classmethod
    def create_source_data_objs_from_workspace(cls, workspace, data_exporting_policy):
        logger.debug(f"workspace.name = {workspace.name}, workspace.type = {workspace.type}, workspace.status = {workspace.status}, workspace.api_key_env_var = {workspace.api_key_env_var}, len(workspace.properties) = {len(workspace.properties)}, creating source_data_objs from workspace")
        source_data_objs = []
        DataObjType_values = [e.value for e in DataObjType]
        try:
            if workspace is not None:

                if workspace.properties is not None:
                    for key in workspace.properties.keys():
                        if key in DataObjType_values and key != DataObjType.LINEAGE.value:#there are different types of properties, only properties with key in DataObjType values stores all the properties of DataObjs, todo: better way but not hardcode to exclude lineage, like try catch to check if a valid DataOjbType
                            property_lsit = workspace.properties[key]
                            for property in property_lsit:
                                data_obj_type = DataObjType(key)
                                data_obj_id = property.get("id")
                                data_obj_name = property.get("name")
                                if data_obj_id is None:
                                    data_obj_id = data_obj_name
                                data_obj_state = DataObjState.INIT
                                data_obj_dependency_discovery = "auto"
                                data_obj_properties = property
                                logger.debug(f"data_obj_name = {data_obj_name}, data_obj_type = {data_obj_type} ")
                                source_data_obj = TPDataObj(data_obj_id, data_obj_name, data_obj_type, data_obj_properties, data_obj_state, workspace, data_obj_dependency_discovery, data_exporting_policy, depends_stream_data_volume = None)
                                source_data_objs.append(source_data_obj)
        except(BaseException) as error:
            logger.debug(f"error = {error}")
            traceback.print_exc()
        logger.info(f"workspace.name = {workspace.name}, workspace.type = {workspace.type}, workspace.status = {workspace.status}, len(source_data_objs) = {len(source_data_objs)} created")
        return source_data_objs   

    @classmethod
    def create_from_spec(cls, spec, workspaces): #workspaces is a list of workspace obj, and then find the soruce_workspace and target_workspaces from tp_exporter
        logger.debug(f"spec = {spec}, workspaces = {workspaces}")
        name = spec.get("name")
        mode = spec.get("mode")
        source_workspace_in_spec = spec.get("source_workspace")
        target_workspaces_in_spec = spec.get("target_workspaces")
        source_workspace = None
        target_workspaces = []
        state = spec.get("status") #get status from spec as the intial state of the operate obj
        source_data_objs = []
        data_exporting_policy = None
        
        #set source_workspace and target_workspaces for TPExportOperate obj
        for workspace in workspaces:
            #logger.debug(f"workspace.name = {workspace.name}, soruce_workspace_in_spec = {source_workspace_in_spec}")
            if workspace.name == source_workspace_in_spec:
                source_workspace = workspace
            else:
                for target_workspace_spec in target_workspaces_in_spec:
                    if workspace.name == target_workspace_spec:
                        target_workspace = workspace
                        target_workspaces.append(target_workspace)
        logger.debug(f"source_workspace = {source_workspace}, target_workspaces = {target_workspaces}")
        if source_workspace is None:
            #raise Exception(f"Error: no workspace setting found for the source workspace = {source_workspace} set in operate = {name}")
            logger.info(f"no source_workspace is found for the TPExporterOperate.name = {name}, skip create_from_spec")
            return None
        else:
            logger.debug(f"source_workspace = {source_workspace.name}")
        if len(target_workspaces) == 0:
            #raise Exception(f"Error: no workspace setting found for the target_workspaces = {target_workspaces} set in operate = {name}")
            logger.info(f"no source_workspace is found for the TPExporterOperate.name = {name}, skip create_from_spec")
            return
        
        #create and set data_exporting_plicy for TPDataObj and TPExportOperate Obj
        data_exporting_policy_spec = spec.get("data_exporting_policy")
        data_exporting_policy = TPExportPolicy.create_from_spec(data_exporting_policy_spec)        
        
        #create and set source_data_objs for TPExportOp
        source_data_objs = cls.create_source_data_objs(spec, source_workspace, data_exporting_policy)
        tp_export_operate_obj = cls(name,mode,source_workspace, state, source_data_objs, data_exporting_policy, target_workspaces, spec)
        return tp_export_operate_obj

    def data_obj_name_in_list(self, data_obj_name, data_obj_list):
        res = -1
        if data_obj_list is None:
            return res
        i = 0
        for item in data_obj_list:
            if isinstance(item, TPDataObj) and item.name == data_obj_name:
                res = i
            i += 1
        logger.debug(f"data_obj_name = {data_obj_name}, res = {res}")
        return res
    

    def dependency_discover(self, data_obj, lineage, dependencies, dependency_streams): #going through lineage recursively to find out the dependency node and put into dependencies and dependency_streams      
        nodes = lineage.get("nodes")
        edges = lineage.get("edges")
        depedencies_len = len(dependencies)
        run_count = 0
        logger.debug(f"begin: data_obj.name = {data_obj.name}, data_obj.type = {data_obj.type}, data_obj.state = {data_obj.state}, dependencies = {dependencies},depedencies_len = {depedencies_len}, dependency_streams = {dependency_streams}, run_count = {run_count}")
        if data_obj.state != DataObjState.DEPENDENCY_DISCOVERED:
            for edge in edges:
                edge_source = edge.get("source")
                edge_target = edge.get("target")
                logger.debug(f"edge_source = {edge_source}, edge_target = {edge_target}, data_obj.name = {data_obj.name}")
                
                if edge_target == data_obj.name: #stream, view name are as same as id and unique, and for the edges, stream and views are in targets, and source won't be in targets, and we only supports, streams, views/mvs for exporting but no sinks and sinks are always in the last even we'd export, so use data_obj.name for now, #todo: fix the dependency check of sink bug, for sink, it uses id in targets
                    edge_source_data_obj = None
                    edge_source_found_in_soruce_data_objs = False
                    for item in self.source_data_objs: #going through source_data_objs, if edge_source is already in then set edge_source_found_in_source_data_objs and break
                        if edge_source == item.id: #id is used for dependency check but not name!
                            edge_source_data_obj = item
                            edge_source_found_in_soruce_data_objs = True
                            break
                    if not edge_source_found_in_soruce_data_objs: #if edge_source is not found in source_data_objs, going through nodes, if the edge_source is a node, create the TPDataObj object, sometimes the edge_source is not a node
                        for node in nodes:
                            node_id = node.get("id")
                            node_name = node.get("name")
                            node_type = node.get("type")
                            if node_id == edge_source:
                                edge_source_data_obj = TPDataObj.create_from_node(node, self.source_workspace, self.data_exporting_policy) #create source_data_obj from lineage node
                                self.source_data_objs.append(edge_source_data_obj) #if the dependency is not in self.source_data_obj list, add it in
                                logger.debug(f"node_id = {node_id}, edge_source = {edge_source}, edge_target = {edge_target},edge_source_found_in_soruce_data_objs = {edge_source_found_in_soruce_data_objs}, len(dependencies) = {len(dependencies)}")

                    if edge_source_data_obj is not None:
                        dependencies.append(edge_source_data_obj)
                        for x in data_obj.dependencies:
                            logger.debug(f"before append data_obj.dependencies, data_obj.name = {data_obj.name}, x.name = {x.name}")
                        logger.debug(f"edge_source = {edge_source}, edge_target = {edge_target}, put {edge_source} into dependencies, len(dependencies) = {len(dependencies)}")                             
                        data_obj.add_dependency(edge_source_data_obj)
                        logger.debug(f"data_obj = {data_obj.name}, edge_source_data_obj = {edge_source_data_obj}, data_obj.dependencies = {data_obj.dependencies}")
                        for x in data_obj.dependencies:
                            logger.debug(f"after append, data_obj.name={data_obj.name}, data_obj.dependencies = {data_obj.dependencies}, x.name = {x.name}")
                        logger.debug(f"edge_source = {edge_source}, edge_target = {edge_target}, put {edge_source} into dependencies, len(dependencies) = {len(dependencies)}")                                    
                        if edge_source_data_obj.type == DataObjType.STREAM:
                            dependency_streams.append(edge_source_data_obj)
                    
            data_obj.state = DataObjState.DEPENDENCY_DISCOVERED
            

        for item in dependencies:
            if isinstance(item, TPDataObj):
                logger.debug(f"dependency = {item.name}")
            else:
                logger.debug(f"dependency = {item}")
        for item in dependency_streams:
            logger.debug(f"depenency_stream = {item.name}")
        
        
        logger.debug(f"len(dependencies) = {len(dependencies)}, dependencies_len = {depedencies_len}")
        if len(dependencies) == depedencies_len: #if no new element is added into dependencies, then stop and return dependencies
            return (dependencies, dependency_streams)
        else:
            for dependency in dependencies:
                if isinstance(dependency, TPDataObj):
                    logger.debug(f"dependency.state = {dependency.state}")
                    if dependency.state != DataObjState.DEPENDENCY_DISCOVERED:
                        self.dependency_discover(dependency, lineage, dependencies, dependency_streams)
                       
    
    def _local_target_workspaces_init(self):
        try:
            _workspace_name_created = []
            for workspace in self.target_workspaces:
                if workspace.type == WorkspaceType.FILE:
                    if not TPWorkSpace.alive_check(workspace): #if no local folder created, create local folder for the target_workspace
                        logger.debug(f"workspace.address = {workspace.address}")
                        workspace_dir_path = os.makedirs(workspace.address)
                        data_dir_path = os.path.join(workspace.address,"data/") #hard code local workspace paths now, todo: make it configureable
                        os.makedirs(data_dir_path)

        except(BaseException) as error:
            logger.debug(f"Exception: error = error")
            traceback.print_exc()          
            return None
    
    
    def schema_export_stats(self):
        try:
            stats = []
            for target_workspace in self.target_workspaces:
                target_workspace_stats = {}
                target_workspace_stats["source_workspace_name"] = self.source_workspace.name
                target_workspace_stats["target_workspace_name"] = target_workspace.name
                target_workspace_stats["summary"] = {}
                target_workspace_stats["summary"]["total_source_obj_number"] = {} #total numbers of the source data objs sources, sinks, views, stremas and udfs
                target_workspace_stats["summary"]["total_source_obj_2_export_number"] = {} # total numbers of the data objs to be exported, views, streams and udfs
                target_workspace_stats["summary"]["total_schema_exported_obj_number"] = {} #total numbers of the data objs exported, views, streams and udfs
                target_workspace_stats["summary"]["total_schema_export_skip_number"] = {} #total numbers of the data objs exported, views, streams and udfs
                target_workspace_stats["summary"]["total_schema_export_failure_number"] = {} #total numbers of the data objs export failure
                target_workspace_stats["failed_schema_export_stats"] = []
                target_workspace_stats["skipped_schema_export_stats"] = []                
                
                for type in DataObjType:
                    target_workspace_stats["summary"]["total_source_obj_number"][type.value] = 0
                    target_workspace_stats["summary"]["total_source_obj_2_export_number"][type.value] = 0
                    target_workspace_stats["summary"]["total_schema_exported_obj_number"][type.value] = 0
                    target_workspace_stats["summary"]["total_schema_export_skip_number"][type.value] = 0
                    target_workspace_stats["summary"]["total_schema_export_failure_number"][type.value] = 0

                for type in DataObjType:
                    for data_obj in self.source_data_objs:
                        if type == data_obj.type:
                            target_workspace_stats["summary"]["total_source_obj_number"][type.value] += 1
                
                stats.append(target_workspace_stats)
            

            for data_obj in self.source_data_objs:
                #target_workspace_stats["summary"]["total_source_obj_number"][data_obj.type.value] += 1
                if data_obj.type == DataObjType.STREAM or data_obj.type == DataObjType.UDF or data_obj.type == DataObjType.VIEW: #only exporting stream, udf and views so far
                    #target_workspace_stats["summary"]["total_source_obj_2_export_number"][data_obj.type.value] += 1
                    for target_workspace_stats in stats:
                        target_workspace_name = target_workspace_stats.get("target_workspace_name")
                        if target_workspace_name in data_obj.schema_export_tracking.keys():
                            target_workspace_stats["summary"]["total_source_obj_2_export_number"][data_obj.type.value] += 1
                            data_obj_schema_export_stat = {}
                            data_obj_schema_export_stat["id"] = data_obj.id
                            data_obj_schema_export_stat["name"] = data_obj.name
                            #logger.debug(f"data_obj.schema_export_tracking[target_workspace_name].keys() = {data_obj.schema_export_tracking[target_workspace_name].keys()}")
                            for key in data_obj.schema_export_tracking[target_workspace_name].keys():
                                #logger.debug(f"data_obj.schema_export_tracking[target_workspace_name][key] = {data_obj.schema_export_tracking[target_workspace_name][key]}, data_obj.type.value = {data_obj.type.value}")
                                data_obj_schema_export_stat[key] = data_obj.schema_export_tracking[target_workspace_name][key]
                                if data_obj.schema_export_tracking[target_workspace_name][key] == DataObjState.SCHEMA_EXPORTED.value:
                                    target_workspace_stats["summary"]["total_schema_exported_obj_number"][data_obj.type.value] += 1
                                elif data_obj.schema_export_tracking[target_workspace_name][key] == DataObjState.SCHEMA_EXPORT_SKIPPED.value:
                                    target_workspace_stats["summary"]["total_schema_export_skip_number"][data_obj.type.value] += 1
                                    target_workspace_stats["skipped_schema_export_stats"].append(data_obj_schema_export_stat)
                                elif data_obj.schema_export_tracking[target_workspace_name][key] == DataObjState.SCHEMA_EXPORT_FAILED.value:
                                    target_workspace_stats["summary"]["total_schema_export_failure_number"][data_obj.type.value] += 1
                                    target_workspace_stats["failed_schema_export_stats"].append(data_obj_schema_export_stat)
                            #workspace_stats["schema_export_stats"].append(data_obj_schema_export_stat)                            
            
            logger.debug(f"stats = {stats}")
            
            return stats
        except(BaseException) as error:
            logger.debug(f"Exception, error = {error}")
            traceback.print_exc()
            return None
            
    def run(self):
        state = self.state
        if state != "enabled":
            logger.info(f"operate.name = {self.name}, disabled, skip")
            return
        
        if self.source_data_objs is not None:
            logger.debug(f"opeate_name = {self.name}, source_data_objs = {self.source_data_objs}, exporting starts...")
            self.state = OperateState.INIT
            target_workspace_init_res = self._local_target_workspaces_init() #check if file type target workspace alive, if not init
            mp_mgr = mp.Manager()
            operate_shared_status = mp_mgr.list() #operate_shared_status[0] for source_objs exported, ..., operate_shared_status[3] for sink_objs exported
            source_objs = []
            stream_objs = []
            view_objs = []
            udf_objs = [] 
            sink_objs = []
            procs = []
            for item in self.source_data_objs:
                if item.type == DataObjType.SOURCE:
                    source_objs.append(item)
                elif item.type == DataObjType.STREAM:
                    stream_objs.append(item)
                elif item.type == DataObjType.VIEW:
                    view_objs.append(item)
                elif item.type == DataObjType.UDF:
                    udf_objs.append(item)
                elif item.type == DataObjType.SINK:
                    sink_objs.append(item)

            # source_data_objs_in_spec = self.spec.get("source_data_objs")
            source_workspace_lineage = self.source_workspace.properties.get("lineage")
            try:
                for source_data_obj in self.source_data_objs: #check the dependency streams of views and put into stream_objs and dedup
                    self.state = OperateState.DEPENDENCY_DISCOVERING                    
                    dependencies = []
                    dependency_streams = []
                    if source_data_obj.type != DataObjType.UDF and source_data_obj.state != DataObjState.DEPENDENCY_DISCOVERED: #udf is not covered by lineage, no depenency discovery
                        self.dependency_discover(source_data_obj,source_workspace_lineage, dependencies, dependency_streams)
                        #source_data_obj.set_dependency(dependencies)
                    for dependency in dependencies: #list dependencies for debug, todo: remove
                        if isinstance(dependency, TPDataObj):
                            logger.debug(f"source_data_obj.name = {source_data_obj.name}, dependency = {dependency.name}")
                            l = dependency.dependencies
                            for item in dependency.dependencies:
                                if isinstance(item, TPDataObj):
                                    logger.debug(f"dependency = {dependency}, dependency of dependency = {item.name}")
                                else:
                                    logger.debug(f"dependency = {dependency}, dependency of dependency = {item} ")
                        else:
                            logger.debug(f"dependency = {dependency}")
                        
                    if dependency_streams is not None and len(dependency_streams) !=0:
                        stream_objs.extend(x for x in dependency_streams if x not in stream_objs)
                self.state = OperateState.DEPENDENCY_DISCOVERED 
            except(BaseException) as error:
                logger.debug(f"Exception, error = {error}")
                traceback.print_exc()
                self.state = OperateState.DEPENDENCY_DISCOVER_FAILED               
            #export streams in multi processes
            self.state = OperateState.STREAM_SCHEMA_EXPORTING
            
            #start streams schema exporting
            try:
                self.state = OperateState.STREAM_SCHEMA_EXPORTING
                for item in stream_objs: 
                    item.exports_schema(self.target_workspaces)
                
                self.state = OperateState.STREAM_SCHEMA_EXPORTED
                
                #start stream data exporting in multiple process mode
            except(BaseException) as error:
                logger.debug(f"Exception, error = {error}")
                traceback.print_exc()
                self.state = OperateState.STREAM_SCHEMA_EXPORT_FAILED
            
            #start multiple process to export stream data
            try:
                self.state = OperateState.STREAM_DATA_EXPORTING
            
                for obj in stream_objs:  
                    #func = item.exports_data
                    proc_dict = {}
                    args = (self.target_workspaces,)
                    proc = mp.Process(target=obj.exports_data, args=args)
                    proc_dict["proc"] = proc
                    proc_dict['obj_name'] = obj.name
                    procs.append(proc_dict)            
                for proc_dict in procs: #starts data exports right after stream schema exporting due to for json type filed, query only is valid when data is ingested
                    proc_dict["proc"].start()
            except(BaseException) as error:
                logger.debug(f"Exception, error = {error}")
                traceback.print_exc()
                self.state = OperateState.STREAM_DATA_EXPORT_FAILED 

            #start udf schema porting
            try:
                self.state = OperateState.UDF_SCHEMA_EXPORTING
                for item in udf_objs: #export UDFs before exporting views due to query may use udfs
                    item.exports_schema(self.target_workspaces)
                self.state = OperateState.UDF_SCHEMA_EXPORTED
            except(BaseException) as error:
                logger.debug(f"Exception, error = {error}")
                traceback.print_exc()
                self.state = OperateState.STREAM_DATA_EXPORT_FAILED             

            #start view schema porting
            try:
                self.state = OperateState.VIEW_SCHEMA_EXPORTING                
                for item in view_objs: #exports views schema
                    item.exports_schema(self.target_workspaces)
            except(BaseException) as error:
                logger.debug(f"Exception, error = {error}")
                traceback.print_exc()
                self.state = OperateState.VIEW_SCHEMA_EXPORTED 


            operate_schema_export_stats = self.schema_export_stats()
            self.tracking[OperateTrackingType.SCHEMA_EXPORT.value] = operate_schema_export_stats

            operate_tracking_str = json.dumps(self.tracking[OperateTrackingType.SCHEMA_EXPORT.value], indent=4)
            logger.info(f"opeate_name = {self.name}, schema exporting done, operate_tracking = \n{operate_tracking_str}")
           
            for proc_dict in procs:
                proc_dict["proc"].join()
            self.state = OperateState.STREAM_DATA_EXPORTED

            logger.debug(f"opeate_name = {self.name}, exporting done...")
        else:
            logger.info(f"operate.name = {self.name}, source_data_objs is None, skip")
            pass
        for workspace in self.target_workspaces:
            logger.info(f"opeate_name = {self.name}, target_workspace = {workspace.name}, import_dependencies = {workspace.import_dependencies}")
        self.state = OperateState.DONE
            
    def __str__(self): 
        _tp_export_operate_str = f"name = {self.name}, source_workspace = {self.source_workspace.name}, state = {self.state},\n "
        if self.source_data_objs is not None:
            _tp_source_data_objs_str = "source_data_objs = [ \n"
            for source_data_obj in self.source_data_objs:
               _tp_source_data_objs_str += f"source_data_obj = {source_data_obj} \n"
            _tp_source_data_objs_str = _tp_source_data_objs_str + "]\n"
            _tp_export_operate_str += _tp_source_data_objs_str
        else:
             _tp_export_operate_str += "source_data_objs = None\n"

        if self.data_exporting_policy is not None:
            _tp_export_operate_str += f"data_exporting_policy = {self.data_exporting_policy}\n"
        else:
            _tp_export_operate_str += "data_exporting_policy = None\n"
        
        if self.target_workspaces is not None:
            _tp_export_operate_str += f"target_workspaces = {self.target_workspaces}\n"
        else:
            _tp_export_operate_str += "target_workspaces = None\n"

        return  _tp_export_operate_str       

class TPExporter(object):
    #hold workspaces in class level
    workspaces = []
    def __init__(self, name, mode, workspaces, operates, spec):
        self.name = name
        self.mode = mode
        self.workspaces = workspaces
        self.operates = operates
        self.sepc = spec


    def __str__(self):

        _tp_exporter_str = f"\nname = {self.name}, \nmode = {self.mode},"
        if self.workspaces is not None:
            _tp_exporter_str += "\nworkspaces = ["
            for workspace in self.workspaces:
                _tp_exporter_str += f"workspace = {workspace.name}\n"
            _tp_exporter_str += "]\n"
        else:
            _tp_exporter_str += "\nworkspaces = None\n"
        if self.operates is not None:
            _tp_exporter_str += "\noperates = ["
            for operate in self.operates:
                _tp_exporter_str += f"\noperate = {operate}\n"
            _tp_exporter_str += "]\n"
        else:
            _tp_exporter_str += "operates = None\n"

        return _tp_exporter_str

    @classmethod
    def create_from_config(cls, config_path):
        tp_export_config = TpExportConfig(config_path)
        #print(f"tp_export_config = {tp_export_config}")
        spec = tp_export_config.dict #todo: config legimate check
        tp_exporter_obj = cls.create_from_spec(spec)
        return tp_exporter_obj
    
    @classmethod
    def create_from_spec(cls,spec):
        name = spec.get('name')
        mode = spec.get("mode")
        workspaces_spec = spec.get("workspaces")
        workspaces = []
        for workspace_spec in workspaces_spec:
            logger.debug(f"workspace creating, workspace.name = {workspace_spec['name']}")
            workspace = TPWorkSpace.create_from_spec(workspace_spec)
            if workspace is not None:
                workspaces.append(workspace)
        logger.debug(f"workspaces = {workspaces}")
        operates = []
        operates_spec = spec.get("operates")
        for operate_spec in operates_spec:
            operate = TPExportOperate.create_from_spec(operate_spec,workspaces)
            operates.append(operate)
        tp_export_obj = cls(name, mode, workspaces, operates, spec)
        return tp_export_obj
    

    def get_operate(self, operate_name):
        operate = None
        for item in self.operates:
            if item.name == operate_name:
                operate = item
                break
        return operate
    
    def run_operate(self, operate_name):
        operate = self.get_operate(operate_name)
        if operate is not None:
            operate.run()
        else:
            logger.info(f"operate_name = {operate_name} not found, skip")
    
    def run(self):
        for item in self.operates:
            item.run()

def main():
    cur_file_path = os.path.dirname(os.path.abspath(__file__))
    cur_file_path_parent = os.path.dirname(cur_file_path)
    test_suite_path = None

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.formatter = formatter
    logger.addHandler(console_handler)

    logger.setLevel(logging.DEBUG)
    #logger.setLevel(logging.INFO)

    config_path = "./config_test.json"
    try:
        tp_exporter = TPExporter.create_from_config(config_path)
        logger.info(f"tp_exporter = {tp_exporter}")
        #tp_exporter.run()
        tp_exporter.run_operate("demo-site-replica-all-2-test")
    except(BaseException) as error:
        logger.info(f"error = {error}")
        traceback.print_exc()


if __name__ == "__main__":
    main()