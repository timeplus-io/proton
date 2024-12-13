import os
import traceback
import logging
import uuid
from timeplus import Stream, Environment
from functools import wraps, lru_cache

# when T+ workspace connection error, retry how many times.
TIMEPLUS_CONNECTION_RETRY = 3


class ProtonHelper:
    def __init__(self, api_address=None, work_space=None, api_key=None):
        if api_address is None:
            api_address = os.environ.get("TIMEPLUS_ADDRESS2")
        if work_space is None:
            work_space = os.environ.get("TIMEPLUS_WORKSPACE2")
        if api_key is None:
            api_key = os.environ.get("TIMEPLUS_API_KEY2")

        if api_address is None or api_key is None:
            logging.error(
                f"one of TIMEPLUS_API_KEY2,TIMEPLUS_ADDRESS2,TIMEPLUS_WORKSPACE2 is not found in ENV")
        logging.warning(f"api_key length: {len(api_key)}, api_address length: {len(api_address)}, work_space length: {len(work_space)}")
        self.env = Environment().address(api_address).workspace(work_space).apikey(api_key)

    def write(
        self,
        record,
    ):
        retry = 0
        retry_flag = True
        while retry < TIMEPLUS_CONNECTION_RETRY and retry_flag:
            try:
                stream_name = record.name
                stream = (
                    Stream(env=self.env)
                    .name(stream_name)
                )
                field_names = record.fields
                row_data = record.data
                logging.debug(
                    f"field_names = {field_names} \n row_data = {row_data}")
                stream.ingest(field_names, row_data)
                retry_flag = False

            except Exception as e:
                print(e)
                traceback.print_exc()
                retry_flag = True
                retry += 1
        return None


def prepare_event(test_results, event_type, **optional_event_msg):
    try:
        event = {"event_type": event_type}
        test_result_flag = True
        detailed_summary = []
        for test_result in test_results:
            test_name = test_result[0]
            test_status = test_result[1]
            current_row = {}
            current_row['test_name'] = test_name
            current_row['test_status'] = test_status
            if "OK" != test_status:
                test_result_flag = test_result_flag * 0
            detailed_summary.append(current_row)
        event['test_result'] = "success" if test_result_flag else "failed"
        if optional_event_msg is not None:
            event.update(optional_event_msg)
        event['payload'] = detailed_summary
    except (BaseException) as error:
        logging.error(f"prepare event Exception = error")
        return None
    return event


def prepare_event_tag(
    version,
    repo_name,
    test_id,
    test_name,
    test_type,
    pr_info,
    **optional_tags,
):
    try:
        if test_id is None:
            test_id = str(uuid.uuid1())
        test_tag = {"version": version, "repo_name": repo_name}
        test_info = {"test_id": test_id,
                     "test_name": test_name, "test_type": test_type}
        build_info = {"build_type": os.environ.get(
            "SANITIZER", "release_build"), "pr_number": pr_info.number, "commit_sha": pr_info.sha}
        run_time_info = {"os": os.getenv(
            "RUNNER_OS", "Linux"), "platform": os.getenv("RUNNER_ARCH", "x86_64")}
        test_tag['test_info'] = test_info
        test_tag['build_info'] = build_info
        test_tag['run_time_info'] = run_time_info
        if optional_tags is not None:
            test_tag.update(optional_tags)
    except (BaseException) as error:
        logging.error(f"prepare event tag Exception = error")
        return None
    return test_tag


class column:

    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        self.fdel(obj)

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.__doc__)

    def setter(self, fset):
        return type(self)(self.fget, fset, self.fdel, self.__doc__)

    def deleter(self, fdel):
        return type(self)(self.fget, self.fset, fdel, self.__doc__)


def stream(name):
    stream_name = name

    def decorator(klass):
        nonlocal stream_name
        if stream_name is None:
            stream_name = klass.__name__
        origin_init = klass.__init__

        @wraps(klass.__init__)
        def wrapper_init(self, *args, **kwargs):
            self.__class__.name = stream_name
            origin_init(self, *args, **kwargs)

        @property
        def name(self):
            return self.__class__.name

        @property
        @lru_cache
        def fields(self):
            return [k for k, v in self.__class__.__dict__.items() if isinstance(v, column)]

        @property
        def data(self):
            return [getattr(self, field) for field in self.fields]

        klass.__init__ = wrapper_init
        klass.name = name
        klass.fields = fields
        klass.data = data
        return klass

    return decorator
