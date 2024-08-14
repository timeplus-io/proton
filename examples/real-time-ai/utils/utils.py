from icecream import ic
import time
import html
import requests
from requests.exceptions import RequestException
import re

from fake_useragent import UserAgent

from unstructured.partition.html import partition_html
from unstructured.cleaners.core import (
    clean,
    replace_unicode_quotes,
    clean_non_ascii_chars,
)
from unstructured.staging.huggingface import chunk_by_attention_window
from unstructured.staging.huggingface import stage_for_transformers

import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

pattern = re.compile("<.*?>")


def safe_request(url, headers={}, wait_time=1, max_retries=3):
    if headers == {}:
        # make a user agent
        ua = UserAgent()

        headers = {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*"
            ";q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referrer": "https://www.google.com/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    current_wait_time = wait_time
    # Send the initial request
    for i in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            html = response.content
            break
        except RequestException as e:
            logger.warning(f"Request failed (attempt {i + 1}/{max_retries}): {e}")
            if i == max_retries:
                logger.warning(f"skipping url {url}")
                html = None
            logger.warning(f"Retrying in {current_wait_time} seconds...")
            time.sleep(current_wait_time)
            i += 1
    return html


def prep_text(metadata_content, tokenizer):
    ic()
    metadata_content["text"] = html.unescape(metadata_content["text"])
    metadata_content["text"] = re.sub(pattern, "", metadata_content["text"])
    metadata_content["text"] = clean_non_ascii_chars(
        replace_unicode_quotes(metadata_content["text"])
    )
    metadata_content["text"] = chunk_by_attention_window(
        metadata_content["text"], tokenizer
    )
    return metadata_content


def parse_html(metadata_content, tokenizer):
    # ic()
    try:
        ic(
            f"parsing content from: {metadata_content['url']} - {metadata_content['content'][:200]}"
        )

        text = []
        article_elements = partition_html(text=metadata_content["content"])
        # breakpoint()
        article_content = clean_non_ascii_chars(
            replace_unicode_quotes(
                clean(
                    " ".join(
                        [
                            " ".join(
                                str(html.unescape(x.text))
                                .replace("\\n", "")
                                .replace("\\t", "")
                                .split()
                            )
                            if x.to_dict()["type"] == "NarrativeText"
                            else ""
                            for x in article_elements
                        ]
                    )
                )
            )
        )
        text += chunk_by_attention_window(article_content, tokenizer)
        # remove the content after it is cleaned
        metadata_content.pop("content")
        return {**metadata_content, "text": text}
    except (TypeError, ValueError):
        logger.warning(f"moving on... can't parse: {metadata_content['content'][:200]}")
        return {**metadata_content, "text": "Failed to parse HTML content."}


def hf_document_embed(document, tokenizer, model, torch, length=384):
    """
    Create an embedding from the provided document
    """
    ic()
    text_chunks = document.pop("text")
    documents = []
    for i, chunk in enumerate(text_chunks):
        inputs = tokenizer(
            chunk, padding=True, truncation=True, return_tensors="pt", max_length=length
        )
        with torch.no_grad():
            embed = model(**inputs).last_hidden_state[:, 0].cpu().detach().numpy()
        documents.append(
            {
                "key_id": f"{document['id']}_{i}",
                **document,
                "text": chunk,
                "doc_embedding": embed.flatten(),
            }
        )
    return documents
