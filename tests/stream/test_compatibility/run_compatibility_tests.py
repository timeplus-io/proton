import inspect
import logging
import math
import os
import sys
import threading
import time
import types
import unittest
import uuid

from ddt import process_file_data
from proton_driver import Client

EPS = 1e-6
FOLDER_ATTR = "%folder_path"
CUR_DIR = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger(__name__)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(funcName)s %(message)s")
console_handler = logging.StreamHandler(sys.stderr)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def folder_data(value):
    def wrapper(func):
        setattr(func, FOLDER_ATTR, value)
        return func

    return wrapper


def copy_func(f, name=None):
    return types.FunctionType(f.func_code, f.func_globals, name or f.func_name, f.func_defaults, f.func_closure)


def using_folder(arg=None, **kwargs):
    def wrapper(cls):
        for name, func in list(cls.__dict__.items()):
            if hasattr(func, FOLDER_ATTR):
                folder_name = getattr(func, FOLDER_ATTR)
                folder = os.listdir(folder_name)
                for filename in folder:
                    filename_without_ext, *_, ext = filename.split(".")
                    full_file_path = os.path.join(folder_name, filename)
                    func_name = f'{name}_{filename_without_ext}_'
                    process_file_data(cls, func_name, func, full_file_path)
                delattr(cls, name)
        return cls

    return wrapper(arg) if inspect.isclass(arg) else wrapper


def check_item_eq(item, expect_item, item_type):
    check_map = [
        ("int", lambda x, y: int(x) == int(y)),
        ("float", lambda x, y: (math.isnan(float(x)) and math.isnan(float(y))) or abs(float(x) - float(y)) < EPS)
    ]
    if expect_item == "any_value":
        return True
    for key_word, check_func in check_map:
        if key_word in item_type:
            return check_func(item, expect_item)
    return str(item) == str(expect_item)


def check_list_eq(row, expect_row, type_row):
    if len(row) != len(expect_row) or len(type_row) != len(expect_row):
        return False
    for val, expect_val, t in zip(row, expect_row, type_row):
        if not check_item_eq(val, expect_val, t):
            return False
    return True


def kill_query(query_id):
    client = Client(
        user="default",
        password="",
        host="localhost",
        port="8463"
    )
    client.execute(f"kill query where query_id='{query_id}'")
    client.disconnect()


@using_folder
class TestCompatibility(unittest.TestCase):

    def run_cmd(self, cmd, expect=None, query_id=None, wait=1, query_time=None, **kwargs):
        logger.info(cmd)
        client = Client(
            user="default",
            password="",
            host="localhost",
            port="8463"
        )
        if query_id is None:
            query_id = str(uuid.uuid4())
        killer = threading.Timer(query_time, kill_query, (query_id,))
        if query_time is not None:
            killer.start()
        if expect is None:
            client.execute(cmd, query_id=query_id)
        else:
            rows = client.execute_iter(cmd, with_column_types=True, query_id=query_id)
            type_row = [t[1] for t in next(rows)]
            for i, expect_row in enumerate(expect):
                row = next(rows)
                self.assertTrue(check_list_eq(row, expect_row, type_row), f"{cmd}\n row [{i}] {row} != {expect_row}")
        client.disconnect()
        killer.cancel()
        time.sleep(wait)

    @folder_data(os.environ.get("TEST_DIR", CUR_DIR))
    def test_compatibility(self, steps, description="", **kwargs):
        logger.info(f"description: {description}")
        for step in steps:
            self.run_cmd(**step)


if __name__ == '__main__':
    unittest.main(verbosity=2)
