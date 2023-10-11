import requests
from datetime import datetime, timedelta
import time, json
from typing import Any, Optional

from bytewax.connectors.periodic import SimplePollingInput
from bytewax.dataflow import Dataflow

import logging

from proton import ProtonOutput

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

class HNInput(SimplePollingInput):
    def __init__(self, interval: timedelta, align_to: Optional[datetime] = None, init_item: Optional[int] = None):
        super().__init__(interval, align_to)
        '''
        By default, only get the recent events
        '''
        if not init_item or init_item == 0:
            self.max_id = int(requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()*0.999998)
        else:
            self.max_id = init_item
        logger.info(f"received starting id: {init_item}")
        

    def next_item(self):
        '''
        Get all the items from hacker news API between the last max id and the current max id.
        '''
        new_max_id = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()
        logger.info(f"current id: {self.max_id}, new id: {new_max_id}. {new_max_id-self.max_id} items to fetch")
        ids = [int(i) for i in range(self.max_id, new_max_id)]
        self.max_id = new_max_id
        logger.debug(ids)
        return ids
    
def download_metadata(hn_id):
    # Given an hacker news id returned from the api, fetch metadata
    logger.info(f"Fetching https://hacker-news.firebaseio.com/v0/item/{hn_id}.json")
    req = requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    )
    if not req.json():
        logger.warning(f"error getting payload from item {hn_id} trying again")
        time.sleep(0.5)
        return download_metadata(hn_id)
    return req.json()

def recurse_tree(metadata):
    try:
        parent_id = metadata["parent"]
        parent_metadata = download_metadata(parent_id)
        return recurse_tree(parent_metadata)
    except KeyError:
        return (metadata["id"], {**metadata, "key_id": metadata["id"]})

def key_on_parent(metadata: dict) -> tuple:
    key, metadata = recurse_tree(metadata)
    return json.dumps(metadata, indent=4, sort_keys=True)
    
def run_hn_flow(init_item): 
    flow = Dataflow()
    flow.input("in", HNInput(timedelta(seconds=15), None, init_item)) # skip the align_to argument
    flow.flat_map(lambda x: x)
    # If you run this dataflow with multiple workers, downloads in
    # the next `map` will be parallelized thanks to .redistribute()
    flow.redistribute()
    flow.map(download_metadata)
    flow.inspect(logger.debug)

    # We want to keep related data together so let's build a 
    # traversal function to get the ultimate parent
    flow.map(key_on_parent)

    flow.output("out", ProtonOutput("hn"))
    return flow
