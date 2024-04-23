"""Output to Timeplus Proton."""
from icecream import ic
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from proton_driver import client
import json
import numpy as np

__all__ = [
    "ProtonSink",
]

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, bytes):
            return "bytes"
        return json.JSONEncoder.default(self, obj)

class _ProtonSinkPartition(StatelessSinkPartition):
    def __init__(self, stream: str, host: str):
        self.client=client.Client(host=host, port=8463)
        self.stream=stream
        sql=f"CREATE STREAM IF NOT EXISTS `{stream}` (raw string)"
        ic(sql)
        self.client.execute(sql)

    def write_batch(self, items):
        rows=[]
        for item in items:
            str=json.dumps(item[0],cls=NumpyEncoder)
            rows.append([str]) # single column in each row
        sql = f"INSERT INTO `{self.stream}` (raw) VALUES"
        self.client.execute(sql,rows)

class ProtonSink(DynamicSink):
    def __init__(self, stream: str, host: str):
        self.stream = stream
        self.host = host if host is not None and host != "" else "127.0.0.1"

    """Write each output item to Proton on that worker.

    Items consumed from the dataflow must look like a string. Use a
    proceeding map step to do custom formatting.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    def build(self, worker_index, worker_count):
        """See ABC docstring."""
        return _ProtonSinkPartition(self.stream, self.host)
