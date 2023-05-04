import uuid
import json
from proton_helper import stream, column


@stream(name="function_test_result")
class FunctionTestRecord():
    def __init__(
        self,
        event_id,
        event,
        tag,
        timestamp,
    ):
        if event_id is None:
            self._event_id = str(uuid.uuid1())
        self._event = event
        self._tag = tag
        self._timestamp = timestamp

    @column
    def event_id(self):
        if self._event_id is not None:
            return self._event_id
        else:
            return None

    @column
    def event(self):
        if self._event is not None:
            return json.dumps(self._event)
        else:
            return None

    @column
    def tag(self):
        if self._tag is not None:
            return json.dumps(self._tag)
        else:
            return None

    @column
    def timestamp(self):
        if self._timestamp is not None:
            return self._timestamp
        else:
            return None
