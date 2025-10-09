import functools
import json
import sys
import time
import traceback
from pathlib import Path

import bottle
from bottle import Bottle, HTTPResponse

kms_provider = Bottle(autojson=True)
"""A mock server for Azure IMDS and GCP metadata"""

from typing import TYPE_CHECKING, Any, Callable, Iterable, cast, overload

if not TYPE_CHECKING:
    from bottle import request
else:
    from typing import Protocol

    class _RequestParams(Protocol):

        def __getitem__(self, key: str) -> str:
            ...

        @overload
        def get(self, key: str) -> 'str | None':
            ...

        @overload
        def get(self, key: str, default: str) -> str:
            ...

    class _HeadersDict(dict[str, str]):

        def raw(self, key: str) -> 'bytes | None':
            ...

    class _Request(Protocol):

        @property
        def query(self) -> _RequestParams:
            ...

        @property
        def params(self) -> _RequestParams:
            ...

        @property
        def headers(self) -> _HeadersDict:
            ...

    request = cast('_Request', None)


def parse_qs(qs: str) -> 'dict[str, str]':
    # Re-use the bottle.py query string parser. It's a private function, but
    # we're using a fixed version of Bottle.
    return dict(bottle._parse_qsl(qs))  # type: ignore


_HandlerFuncT = Callable[
    [],
    'None|str|bytes|dict[str, Any]|bottle.BaseResponse|Iterable[bytes|str]']


def handle_asserts(fn: _HandlerFuncT) -> _HandlerFuncT:
    "Convert assertion failures into HTTP 400s"

    @functools.wraps(fn)
    def wrapped():
        try:
            return fn()
        except AssertionError as e:
            traceback.print_exc()
            return bottle.HTTPResponse(status=400,
                                       body=json.dumps({'error':
                                                        list(e.args)}))

    return wrapped


def test_params() -> 'dict[str, str]':
    return parse_qs(request.headers.get('X-MongoDB-HTTP-TestParams', ''))

@kms_provider.get('/computeMetadata/v1/instance/service-accounts/default/token')
@handle_asserts
def get_gcp_token():
    metadata_header = request.headers.get("Metadata-Flavor")
    assert metadata_header == 'Google'

    case = test_params().get('case')
    print('Case is:', case)
    if case == '404': 
        return HTTPResponse(status=404)
    
    if case == 'bad-json':
        return b'{"access-token": }'
    
    if case == 'empty-json':
        return b'{}'

    if case == 'giant':
        return _gen_giant()

    if case == 'slow':
        return _slow()

    assert case in (None, ''), 'Unknown HTTP test case "{}"'.format(case)
    
    return {
        'access_token' : 'google-cookie',
        'token_type' : 'Bearer'
    }

@kms_provider.get('/metadata/identity/oauth2/token')
@handle_asserts
def get_oauth2_token():
    api_version = request.query['api-version']
    assert api_version == '2018-02-01', 'Only api-version=2018-02-01 is supported'
    resource = request.query['resource']
    assert resource == 'https://vault.azure.net', 'Only https://vault.azure.net is supported'

    case = test_params().get('case')
    print('Case is:', case)
    if case == '404':
        return HTTPResponse(status=404)

    if case == '500':
        return HTTPResponse(status=500)

    if case == 'bad-json':
        return b'{"key": }'

    if case == 'empty-json':
        return b'{}'

    if case == 'giant':
        return _gen_giant()

    if case == 'slow':
        return _slow()

    assert case in (None, ''), 'Unknown HTTP test case "{}"'.format(case)

    return {
        'access_token': 'magic-cookie',
        'expires_in': '70',
        'token_type': 'Bearer',
        'resource': 'https://vault.azure.net',
    }


def _gen_giant() -> Iterable[bytes]:
    "Generate a giant message"
    yield b'{ "item": ['
    for _ in range(1024 * 256):
        yield (b'null, null, null, null, null, null, null, null, null, null, '
               b'null, null, null, null, null, null, null, null, null, null, '
               b'null, null, null, null, null, null, null, null, null, null, '
               b'null, null, null, null, null, null, null, null, null, null, ')
    yield b' null ] }'
    yield b'\n'


def _slow() -> Iterable[bytes]:
    "Generate a very slow message"
    yield b'{ "item": ['
    for _ in range(1000):
        yield b'null, '
        time.sleep(1)
    yield b' null ] }'


if __name__ == '__main__':
    print(
        'RECOMMENDED: Run this script using bottle.py (e.g. [{} {}/bottle.py fake_kms_provider_server:kms_provider])'
        .format(sys.executable,
                Path(__file__).resolve().parent))
    kms_provider.run()
