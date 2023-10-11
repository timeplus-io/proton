"""Output to Timeplus Proton."""
from bytewax.outputs import DynamicOutput, StatelessSink
from proton_driver import client
import logging

__all__ = [
    "ProtonOutput",
]
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

class _ProtonSink(StatelessSink):
    def __init__(self, stream: str):
        self.client=client.Client(host='127.0.0.1', port=8463)
        self.stream=stream
        sql=f"CREATE STREAM IF NOT EXISTS `{stream}` (raw string)"
        logger.debug(sql)
        self.client.execute(sql)

    def write_batch(self, items):
        rows=[]
        for item in items:
            rows.append([item]) # single column in each row
        sql = f"INSERT INTO `{self.stream}` (raw) VALUES"
        # logger.debug(f"inserting data {sql}")
        self.client.execute(sql,rows)

class ProtonOutput(DynamicOutput):
    def __init__(self, stream: str):
        self.stream=stream
    
    """Write each output item to Proton on that worker.

    Items consumed from the dataflow must look like a string. Use a
    proceeding map step to do custom formatting.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    def build(self, worker_index, worker_count):
        """See ABC docstring."""
        return _ProtonSink(self.stream)