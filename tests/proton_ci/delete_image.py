import requests
import os
from env_helper import GH_PERSONAL_ACCESS_TOKEN

PROTON_VERSION = os.getenv("PROTON_VERSION")
HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GH_PERSONAL_ACCESS_TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28",
}

package_versions = requests.get(
    f"https://api.github.com/orgs/timeplus-io/packages/container/proton/versions",
    headers=HEADERS,
).json()

version_id = None
for package_version in package_versions:
    if PROTON_VERSION in package_version["metadata"]["container"]["tags"]:
        version_id = package_version["id"]

if version_id is not None:
    delete_result = requests.delete(
        f"https://api.github.com/orgs/timeplus-io/packages/container/proton/versions/{version_id}",
        headers=HEADERS,
    )

    if delete_result.status_code == 204:
        print(f"delete {PROTON_VERSION} success")
    else:
        print(f"delete error, result: {delete_result.text}")
else:
    print("image not found")
