from icecream import ic
import requests
from datetime import timedelta
import time
import os
from typing import Any, Optional, Tuple
import logging
import atexit

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource

from utils.utils import safe_request, parse_html, hf_document_embed, prep_text

from transformers import AutoTokenizer, AutoModel
import torch

from proton import ProtonSink

VECTOR_DIMENSIONS=384

tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HNSource(SimplePollingSource):
    def next_item(self):
        return ic(
            "GLOBAL_ID",
            requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json(),
        )

def get_id_stream(old_max_id, new_max_id) -> Tuple[str,list]:
    if old_max_id is None:
        # Get the last 150 items on the first run.
        old_max_id = new_max_id - 150
    return ic(new_max_id, range(old_max_id, new_max_id))

def download_metadata(hn_id) -> Optional[dict]:
    data = requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    ).json()

    if data is None:
        logger.warning(f"Couldn't fetch item {hn_id}, skipping")
        return None

    if data["type"] in ["story", "comment"] and not data.get("deleted") and not data.get("dead"):
        return data
    else:
        # logger.warning(f"unknown item {hn_id}, skipping it")
        return None

def download_html(metadata):
    ic()
    try:
        html = safe_request(metadata["url"])
        return {**metadata, "content": html}
    except KeyError:
        logger.error(f"No url content for {metadata}")
        return None


def recurse_tree(metadata, og_metadata=None) -> any:
    if not og_metadata:
        og_metadata = metadata
    try:
        parent_id = metadata["parent"]
        parent_metadata = download_metadata(parent_id)
        if parent_metadata is None:
            raise ValueError("download_metadata returned None for parent_id: {parent_id}")
        return recurse_tree(parent_metadata, og_metadata)
    except KeyError:
        return {**og_metadata, "root_id": metadata["id"]}
    except ValueError:
        return None

# main entry
def run_hn_flow(polling_interval=15):
    flow = Dataflow("hn_stream")
    ic()
    max_id = op.input(
        "in", flow, HNSource(timedelta(seconds=polling_interval))
    )
    id_stream = op.stateful_map("range", max_id, lambda: None, get_id_stream).then(
    op.flat_map, "strip_key_flatten", lambda key_ids: key_ids[1]).then(op.redistribute, "scaling")


    enriched = op.filter_map("enrich", id_stream, download_metadata)

    branch_out = op.branch(
        "split_comments", enriched, lambda document: document["type"] == "story"
    )

    # Stories
    # stories = branch_out.trues
    # stories_htmls = op.filter_map("fetch_html", stories, download_html)
    # stories_parsed = op.filter_map(
    #     "parse_docs", stories_htmls, lambda content: parse_html(content, tokenizer)
    # )
    # stories_embeddings = op.map(
    #     "story_embeddings",
    #     stories_parsed,
    #     lambda document: hf_document_embed(
    #         document, tokenizer, model, torch, length=VECTOR_DIMENSIONS
    #     ),
    # )

    # Comments
    comments = branch_out.falses
    comments = op.filter_map("read_parent", comments, recurse_tree)
    comments = op.map(
        "clean_text", comments, lambda document: prep_text(document, tokenizer)
    )
    comments = op.map(
        "comment_embeddings",
        comments,
        lambda document: hf_document_embed(
            document, tokenizer, model, torch, length=VECTOR_DIMENSIONS
        ),
    )

    # op.output(
    #     "stories_out",
    #     stories_embeddings,
    #     ProtonSink("hn_stories_raw", os.environ.get("PROTON_HOST","127.0.0.1"))
    # )
    op.output(
        "comments_out",
        comments,
        ProtonSink("hn_comments_raw", os.environ.get("PROTON_HOST","127.0.0.1"))
    )
    return flow
