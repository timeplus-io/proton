import requests
import os


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
USERTYPE = "orgs"
USERNAME = "timeplus-io"
PACKAGE_TYPE = "container"
PACKAGE_NAME = "proton"
PROTON_VERSION = os.getenv("PROTON_VERSION")
HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28",
}
package_versions = requests.get(
    f"https://api.github.com/{USERTYPE}/{USERNAME}/packages/{PACKAGE_TYPE}/{PACKAGE_NAME}/versions",
    headers=HEADERS,
).json()
version_id = -1
for package_version in package_versions:
    if PROTON_VERSION in package_version["metadata"]["container"]["tags"]:
        version_id = package_version["id"]
if version_id != -1:
    delete_result = requests.delete(
        f"https://api.github.com/{USERTYPE}/{USERNAME}/packages/{PACKAGE_TYPE}/{PACKAGE_NAME}/versions/{version_id}",
        headers=HEADERS,
    )
    if delete_result.status_code == 204:
        print(f"delete {PROTON_VERSION} success")
    else:
        print(f"delete error, result:{delete_result.text}")
else:
    print("image not found")

