import os
import httpx
from datetime import datetime
from httpx import USE_CLIENT_DEFAULT
from proton_driver import client
from openai import OpenAI

proton_host = os.getenv("PROTON_HOST")
proton_user = os.getenv("PROTON_USER")
proton_password = os.getenv("PROTON_PASSWORD")

c = client.Client(host=proton_host, port=8463, user=proton_user,password=proton_password)

class TimeplusStreamLogHTTPClient(httpx.Client):
    def send(
        self,
        request,
        *,
        stream: bool = False,
        auth: None = USE_CLIENT_DEFAULT,
        follow_redirects: bool = True,
        **kwargs
    ) -> httpx.Response:                
        request_time = datetime.now()
        response = super().send(request, stream=stream, auth=auth, follow_redirects=follow_redirects, **kwargs)

        data = [
            [
                request.method,
                str(request.url),
                {
                    str(key): str(value) for key, value in response.headers.items()
                },
                request.content.decode('utf-8') if request.content else "",
                request_time,
                datetime.now(),
                response.status_code,
                response.text
            ]
        ]

        try:
            c.execute(
                'INSERT INTO llm_calls (request_method, request_url, request_headers, request_content, request_time, response_time, response_status_code, response_text) VALUES',
                data
            )
        except Exception as e:
            print("failed to log llm call to timeplus")
        
        return response

# set the http_client with an instance of TimeplusStreamLogHTTPClient to log all calls to OpenAI
log_client = TimeplusStreamLogHTTPClient()
client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        http_client=log_client
    )