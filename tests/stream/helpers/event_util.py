import os, sys, logging, datetime, time, uuid, traceback, json
from timeplus import Stream, Environment

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)8s] [%(processName)s] [%(module)s] [%(funcName)s] %(message)s (%(filename)s:%(lineno)s)"
)

TIMEPLUS_CONNECTION_RETRY = 3; #when T+ workspace connection error, retry how many times.



# class SettingEvent(Event):
#     def __init__(self, test_id, setting, event_type, detailed_type, details, tag = {}, version = '0.1'):
#         self._test_id = test_id
#         setting_event = {"detailed_type": detailed_type, "details": details}
#         self._setting = setting
#         setting_info = {"setting_info": setting}
#         test_info 
#         self._setting_info = setting_info
#         tag = {**setting_info, **tag}
#         self._setting_event = setting_event

class TestInfoTag():
    def __init__(self, test_id, test_name, test_type, **optional_test_info_tags):
        if test_id is None:
            self._test_id = str(uuid.uuid1())
        else:
            self._test_id = test_id        
        self._test_name = test_name
        self._test_type = test_type
        if optional_test_info_tags is not None:
            test_info = {"test_info": {**{"test_id": self._test_id, "test_name": self._test_name, "test_type": test_type}, **optional_test_info_tags}}
        else:
            test_info = {"test_info": {"test_id": self._test_id, "test_name": self._test_name, "test_type": test_type}}
        self._value = test_info

    @property
    def test_id(self):   
        if self._test_id is not None:
            return self._test_id
        else:
            return None 

    @property
    def test_name(self):   
        if self._test_name is not None:
            return self._test_name
        else:
            return None 

    @property
    def test_type(self):   
        if self._test_type is not None:
            return self._test_type
        else:
            return None

    @property
    def value(self):   
        if self._value is not None:
            return self._value
        else:
            return None 

    def __str__(self): 
        return f"{self._value}"

class BuildInfoTag():
    def __init__(self, build_type, pr_number, commit_sha, **optional_build_info_tags):
        self._build_type = build_type
        self._pr_number = pr_number
        self._commit_sha = commit_sha
        if optional_build_info_tags is not None:
            build_info = {"build_info": {**{"build_type": build_type, "pr_number": pr_number, "commit_sha": commit_sha},**optional_build_info_tags}}
        else:
            build_info = {"build_info": {"build_type": build_type, "pr_number": pr_number, "commit_sha": commit_sha}}
        self._value = build_info    
    @property
    def value(self):   
        if self._value is not None:
            return self._value
        else:
            return None 

    def __str__(self): 
        return f"{self._value}"
    
class RunTimeInfoTag():
    def __init__(self, os_info, platform_info, **optional_run_time_info_tags):
        self._os_info = os_info
        self._platform_info = platform_info
        if optional_run_time_info_tags is not None:
            run_time_info = {"run_time_info": {**{"os":os_info, "platform":platform_info}, **optional_run_time_info_tags}}
        else:
            run_time_info = {"run_time_info": {"os":os_info, "platform":platform_info}}
        self._value = run_time_info    
    @property
    def value(self):   
        if self._value is not None:
            return self._value
        else:
            return None 

    def __str__(self): 
        return f"{self._value}"

class TestEventTag:
    def __init__(self, repo_name, test_info_tag, build_info_tag, runtime_info_tag, **optional_tags):
        self._test_info_tag = test_info_tag
        self._build_info_tag = build_info_tag
        self._runtime_info_tag = runtime_info_tag
        if optional_tags is not None:
            tag = optional_tags
        self._repo_name = repo_name       
        repo_name = {"repo_name": repo_name} #compose repo_name        
        value = {**repo_name, **test_info_tag.value, **build_info_tag.value, **runtime_info_tag.value, **tag}
        self._value = value

    @property
    def test_info_tag(self):   
        if self._test_info_tag is not None:
            return self._test_info_tag
        else:
            return None 

    @property
    def build_info_tag(self):   
        if self._build_info_tag is not None:
            return self._build_info_tag
        else:
            return None 

    @property
    def runtime_info_tag(self):   
        if self._runtime_info_tag is not None:
            return self._runtime_info_tag
        else:
            return None 

    @property
    def value(self):   
        if self._value is not None:
            return self._value
        else:
            return None 


    def __str__(self): 
        return f"{self._value}"         

    @classmethod
    def create(cls, repo_name, test_id, test_name, test_type, build_type, pr_number, commit_sha, os_info, platform_info, **optional_tags):
        try:
            test_info_tag = TestInfoTag(test_id, test_name, test_type)
            build_info_tag = BuildInfoTag(build_type, pr_number, commit_sha)
            runtime_info_tag = RunTimeInfoTag(os_info, platform_info)
            test_event_tag = TestEventTag(repo_name, test_info_tag, build_info_tag, runtime_info_tag, **optional_tags)
            return test_event_tag
        except(BaseException) as error:
            print(f"TestEvent.create Exception = error")
            traceback.print_exc()
            return None    

class TestSuiteInfoTag():
    def __init__(self, test_suite_id, test_suite_name, setting_config, test_suite_config, **optional_test_suite_tags):
        if test_suite_id is None:
            test_suite_id = str(uuid.uuid1())
        self._test_suite_name = test_suite_name
        self._setting_config = setting_config
        self._test_suite_config = test_suite_config
        test_suite_info_tag = {"test_suite_id": test_suite_id,"test_suite_name": test_suite_name,"setting_config": setting_config, "test_suite_config": test_suite_config}
        if optional_test_suite_tags is not None:
            test_suite_info_tag = {"test_suite_info": {**test_suite_info_tag, **optional_test_suite_tags}}
        else:
            test_suite_info_tag = {"test_suite_info": {"test_suite_id": test_suite_id,"test_suite_name": test_suite_name,"setting_config": setting_config, "test_suite_config": test_suite_config}}

        self._value = test_suite_info_tag

    @property
    def value(self):   
        if self._value is not None:
            return self._value
        else:
            return None 

    def __str__(self): 
        return f"{self._value}" 


class TestSuiteEventTag():
    def __init__(self,test_event_tag_dict, test_suite_info_tag_dict, **optional_test_suite_event_tags):
        self._test_event_tag = test_event_tag_dict
        self._test_suite_tag = test_suite_info_tag_dict
        self._optional_test_suite_event_tags = optional_test_suite_event_tags
        if optional_test_suite_event_tags is not None:
            value = {**test_event_tag_dict, **test_suite_info_tag_dict, **optional_test_suite_event_tags}
        else:
            value = {**test_event_tag_dict, **test_suite_info_tag_dict}       
        
        self._value = value
    @property
    def value(self):   
        if self._value is not None:
            return self._value
        else:
            return None 


    def __str__(self): 
        return f"{self._value}" 
    

class Event():
    def __init__(self, event_type, detailed_type, details, **optional_event_msg):
        self._event_type = event_type
        self._detailed_type = detailed_type
        self._details = details
        self._optional_event_msg = optional_event_msg
        event_payload = {"detailed_type": detailed_type, "details": details}
        if optional_event_msg is not None:
            event_payload = {**event_payload, **optional_event_msg}
        self._event_payload = event_payload
        value = {"event_type": event_type, "payload":event_payload}
        self._value = value #compose event 
    @property
    def value(self):   
        if self._value is not None:
            return self._value
        else:
            return None
    def __str__(self): 
        return f"{self._value}"
    @classmethod
    def create(cls, event_type, event_detailed_type, event_details, **optional_event_msg):
        try:
            event = Event(event_type, event_detailed_type, event_details, **optional_event_msg)
            return event
        except(BaseException) as error:
            print(f"TestEvent.create Exception = error")
            traceback.print_exc()
            return None                

class EventRecord():
    def __init__(self, event_id, event_obj, tag_obj, version):
        if event_id is None:
            self._event_id = str(uuid.uuid1())
        self._event = event_obj
        version_dict = {"version":str(version)}
        self._tag = tag_obj
        if tag_obj is None:
            self._tag_value = version_dict
        else:
            self._tag_value = {**version_dict, **tag_obj.value} #todo: json validate
        self._timestamp = str(datetime.datetime.now())

        self._version = version
    
    @property
    def event_id(self):   
        if self._event_id is not None:
            return self._event_id
        else:
            return None

    @property
    def event(self):   
        if self._event is not None:
            return self._event
        else:
            return None

    @property
    def tag(self):   
        if self._tag is not None:
            return self._tag
        else:
            return None



    @property
    def version(self):   
        if self._version is not None:
            return self._version
        else:
            return None        


    @property
    def timestamp(self):   
        if self._timestamp is not None:
            return self._timestamp
        else:
            return None

    def __str__(self): 
        return "{"+ f"event_id = {self._event_id}, event = {self._event}, tag = {self._tag},version = {self._version}, timestamp = {self._timestamp}" + "}"
    
    def write(
        self,
        env,  
        stream_name
    ):
        retry = 0
        retry_flag = True
        while retry <= TIMEPLUS_CONNECTION_RETRY and retry_flag:
            try:
                #env = Environment().address(api_address).workspace(work_space).apikey(api_key)
                stream = (
                    Stream(env=env)
                    .name(stream_name)
                    .column("event_id", "string")
                    .column("event", "string")
                    #.column("test_info", "string")
                    .column("tag", "string")
                    .column("timestamp", "datetime64(3)")
                    
                )
                field_names = ["event_id", "event", "tag", "timestamp"]
                print(f"self._event.value = {self._event.value}")
                print(f"self._tag_value = {self._tag_value}")  
                row_data = [
                    self._event_id,
                    json.dumps(self._event.value),
                    json.dumps(self._tag_value),
                    self._timestamp,

                ]
                
                logger.debug(f"field_names = {field_names} \n row_data = {row_data}")

                stream.ingest(field_names, [row_data])
                retry_flag = False
                return self                              

            except Exception as e:
                print(e)
                traceback.print_exc()
                retry_flag = True
                retry += 1        
        return None

    @classmethod
    def create(cls, event_id, event, tag, version):
        try:
            event = EventRecord(event_id, event, tag, version)
            return event
        except(BaseException) as error:
            print(f"TestEvent.create Exception = error")
            traceback.print_exc()
            return None 


# class TestEvent(EventRecord):
#     def __init__(self, test_event_tag, event, version, **optional_tags):
#         self._test_event_tag = test_event_tag
#         if optional_tags is not None:
#             tag = {**test_event_tag.tag, **optional_tags}
#         else:
#             tag = test_event_tag
#         self._tag = tag #compose tag
#         self._event = event
#         super().__init__(None, event, tag, version)   
    
#     @property
#     def test_event_tag(self):   
#         if self._test_event_tag is not None:
#             return self._test_event_tag
#         else:
#             return None 
    
#     @classmethod
#     def create(cls, test_event_tag, event, version, **optional_tags):
#         try:
#             test_event = TestEvent(test_event_tag, event, version, **optional_tags)
#             return test_event
#         except(BaseException) as error:
#             print(f"TestEvent.create Exception = error")
#             traceback.print_exc()
#             return None                                      

# def test_event_write(
#     test_id,
#     repo_name,
#     test_name,
#     test_type,
#     event_detailed_type,
#     event_details,
#     timeplus_env,
#     timeplus_stream_name,
#     event_type = 'test_event',
#     tag = {},
#     build_type = 'sanitizer',
#     pr_number='0',
#     commit_sha='0',
#     os_info='linux',
#     platform_info='x86',
#     version="0.1"
# ): #todo: have a class for the tag, details
#     print(f" tag = {tag} \n")
#     build_info = {"build_info": {"build_type": build_type, "pr_number": pr_number, "commit_sha": commit_sha}}
#     run_time_info = {"run_time_info": {"os":os_info, "platform":platform_info}}
#     tag = {**build_info, **run_time_info, **tag}
#     test_event = TestEvent(test_id, repo_name, test_name, test_type, event_type, event_detailed_type, event_details, tag, version)
#     logger.debug(f"test_event = {test_event}")
#     res = test_event.write(timeplus_env, timeplus_stream_name)
#     return res


# class TestPerSetEvent(TestEvent):
#     def __init__(self, setting_id, setting_name, test_suites, test_id, repo_name, test_name, test_type, event_type, detailed_type, details, tag = {}, version = '0.1'):
#         if setting_id is None:
#             self._setting_id = str(uuid.uuid1())
#         else:
#             self._setting_id = setting_id
#         self._setting_name = setting_name
#         self._test_suites = test_suites
#         setting_info = {"setting_info": {"setting_id": setting_id, "setting_name": setting_name, "test_suites":test_suites}}
#         tag = {**setting_info, **tag}             
#         super().__init__(test_id, repo_name, test_name, test_type, event_type, detailed_type, details, tag = {}, version = '0.1')    

#     @property
#     def setting_id(self):   
#         if self._setting_id is not None:
#             return self._setting_id
#         else:
#             return None

#     @property
#     def setting_name(self):   
#         if self._setting_name is not None:
#             return self._setting_name
#         else:
#             return None

#     @property
#     def test_suites(self):   
#         if self._test_suites is not None:
#             return self._test_suites
#         else:
#             return None

#     def __str__(self):
#         super_str_res = super().__str__() 
#         return (f"self.setting_id = {self._setting_id}, self.setting_name = {self._setting_name}, self.test_suites = {self._test_suites}" + super_str_res)    

#     def write(
#         self,
#         env,  
#         stream_name
#     ):
        
#         test_id = super().write(env, stream_name)
#         #os.environ["TIMPLUS_TEST_ID"] = str(self._test_id) #set env var for test_id
#         print(f"TestPerSetEvent write: self._setting_id = {self._setting_id}")
#         return self._setting_id

if __name__ == "__main__":
    #test_event = Event('proton', 'test_event', {"detailed-type": "test_status", "detail": {"status":"start"}}, 'a0001', {"source": "ci_node_1", "source_type":"ci", "platform": "linux", "build_type": "sanitizer"})
    
    setting_name = "nativelog"
    test_suites = "cte,smoke"
    repo_name = 'proton'
    test_name = 'ci_smoke_001'
    test_type = 'ci_smoke'
    event_type = 'test_setting_run_event' #for a tese run level event, test_start, test_init, test_end
    detailed_type = 'status' #detaild type is different for differnt event_type, for test_event, only status is used for now.
    details = 'start'
    stream_name = 'test_event_2'
    #tag = {"build_info": {"build_type": "sanitizer", "pr_number": "0", "commit_sha": "0"}, "run_time_info":{"os":"linux", "platform":"x86"}}

    #test_event = TestEvent(repo_name, test_name, test_type, event_type, detailed_type, details, tag)

    

    #print(f"test_event = {test_event}")
    api_key = os.environ.get("TIMEPLUS_API_KEY2")
    api_address = os.environ.get("TIMEPLUS_ADDRESS2")
    work_space = os.environ.get("TIMEPLUS_WORKSPACE2")
    env = Environment().address(api_address).workspace(work_space).apikey(api_key)

    test_setting_event1 = TestPerSetEvent(None,setting_name, test_suites, None, repo_name, test_name, test_type, event_type, detailed_type, details)
    print(f"test_setting_event1 = {test_setting_event1}")
    test_setting_event1.write(env,'test_event_2')    
    #test_event.write(env, 'test_event_2')
    #test_event_write(repo_name, test_name, test_type, detailed_type, details, env, stream_name)
