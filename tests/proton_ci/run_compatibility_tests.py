import json
import logging
import time
from typing import List, Any

import requests
from env_helper import GH_PERSONAL_ACCESS_TOKEN, PROTON_VERSION
import re

HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GH_PERSONAL_ACCESS_TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28"
}
MAX_TEST_VERSION_NUM = 5


def valid_version_tag(tag_list: List[str]) -> Any:
    for tag in tag_list:
        if re.match(r"\d+\.\d+\.\d+", tag) is not None:
            return tag
    return None


if __name__ == "__main__":
    proton_image_version_list = requests.get(
        url="https://api.github.com/orgs/timeplus-io/packages/container/proton/versions?per_page=100",
        headers=HEADERS
    ).json()

    valid_version_list = []
    current_version = "latest"
    for version_info in proton_image_version_list:
        created_at = time.mktime(time.strptime(version_info['created_at'], "%Y-%m-%dT%H:%M:%SZ"))
        tags = version_info['metadata']['container']['tags']
        tag_name = valid_version_tag(tags)
        if tag_name is None:
            continue
        if tag_name == PROTON_VERSION or tag_name == PROTON_VERSION + "-rc":
            current_version = tag_name
        valid_version_list.append((created_at, tag_name))

    valid_version_list.sort(key=lambda version_tuple: -int(version_tuple[0]))
    valid_version_list = valid_version_list[:min(len(valid_version_list), MAX_TEST_VERSION_NUM + 1)]

    for _, version in valid_version_list:
        try:
            response = requests.post(
                "https://api.github.com/repos/timeplus-io/proton/actions/workflows/compatibility_test.yml/dispatches",
                headers=HEADERS,
                data=json.dumps({
                    "ref": "develop",
                    "inputs": {
                        "source": version,
                        "target": current_version
                    }
                })
            )
            assert response.status_code == 204
        except Exception as e:
            logging.error(e)
