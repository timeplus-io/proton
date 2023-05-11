import os, sys, logging, datetime, time, uuid, traceback, json
import swagger_client
from swagger_client.rest import ApiException

import timeplus
from timeplus import Environment, Stream

if __name__ == '__main__':
    api_key = os.environ.get("TIMEPLUS_API_KEY")
    env = Environment().address("https://dev.timeplus.cloud/tp-eng").apikey(api_key)
    #stream_list = Stream(env=env).list()
    #print(f"stream_list = {stream_list}")
    top_apiclient = swagger_client.TopologyV1beta2Api(
            swagger_client.ApiClient(env._conf())
        )
    topo_response = top_apiclient.v1beta2_topology_get()
    edges = topo_response.edges
    nodes = topo_response.nodes
    for edge in edges:
        print(edge)
        print(edge.to_dict().get("source"))
    
