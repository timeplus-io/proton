import requests
import logging
import re
import json
from packaging.version import Version, InvalidVersion, parse

from env_helper import GH_PERSONAL_ACCESS_TOKEN, SANITIZER, ARCH, PROTON_VERSION

MAX_PER_PAGE_NUM = 100
WEEK_PER_PAGE_NUM = 7


class GitHubAPI:
    def __init__(self):
        self.repo = "timeplus-io/proton"
        self.token = GH_PERSONAL_ACCESS_TOKEN
        self.base_url = f"https://api.github.com/repos/{self.repo}"

    def _get_headers(self):
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28"
        }

    def get_latest_release(self, per_page=1, page=1):
        url = f"{self.base_url}/releases?per_page={per_page}&page={page}"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            return None

    def get_release_by_tag(self, tag):
        url = f"{self.base_url}/releases/tags/{tag}"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            return None

    def get_latest_workflow_run(self, workflow_id, per_page=1):
        url = f"{self.base_url}/actions/workflows/{workflow_id}/runs?per_page={per_page}"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            runs = response.json().get('workflow_runs', [])
            return runs if runs else None
        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            return None

    def dispatch_workflow(self, workflow_id, ref, inputs=None):
        url = f"{self.base_url}/actions/workflows/{workflow_id}/dispatches"
        data = json.dumps({
            "ref": ref,
            "inputs": inputs or {}
        })
        response = requests.post(url, data=data, headers=self._get_headers())
        if response.status_code == 204:
            logging.info(f"Workflow {workflow_id} dispatched successfully.")
        else:
            logging.error(f"Error: {response.status_code} - {response.text}")

    def get_workflow_run_jobs(self, run_id, per_page=MAX_PER_PAGE_NUM):
        url = f"{self.base_url}/actions/runs/{run_id}/jobs?per_page={per_page}"
        response = requests.get(url, headers=self._get_headers())
        if response.status_code == 200:
            return response.json().get('jobs', [])
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    def is_job_successful(self, run_id, job_name):
        jobs = self.get_workflow_run_jobs(run_id)
        if jobs is not None:
            for job in jobs:
                if job['name'] == job_name:
                    return job['conclusion'] == 'success'
        return False


class VersionHelper:
    def __init__(self):
        self.github_api = GitHubAPI()

    def tag_to_version(self, tag):
        return tag.lstrip("v")

    def version_to_tag(self, version):
        return f"v{version}"

    def is_release(self, version):
        try:
            return not Version(version).is_prerelease
        except InvalidVersion:
            return False

    def get_version_list(self, version_list_str):
        if not version_list_str:
            return []
        return version_list_str.split(',')

    def replace_version_list(self, versions, specified):
        for version in specified:
            if version not in versions:
                versions.pop()
                versions.insert(0, version)
        return versions

    def get_latest_release_version(self, num=MAX_PER_PAGE_NUM):
        version_list = []
        page=1
        while True:
            releases = self.github_api.get_latest_release(num, page)
            if not releases:
                break
            for release in releases:
                tag_name = release.get("tag_name")
                if tag_name is None:
                    continue
                version_name = str(self.tag_to_version(tag_name))
                if version_name is None or not self.is_release(version_name):
                    continue
                version_list.append(version_name)
            page+=1
        return version_list

    def get_previous_release_version(self, current_version, num=MAX_PER_PAGE_NUM, include_current=False):
        version_list = self.get_latest_release_version()
        version_list.sort(key=lambda x: Version(x), reverse=True)
        print(f"current version: {current_version}")
        print(f"full latest release version: {version_list}")

        try:
            current_ver = parse(current_version)
        except Exception:
            print(f"current version ({current_version}) does not conform to the SemVer standard, select latest version as previous version")
            return version_list[:num]

        previous = [v for v in version_list if Version(v) < current_ver]
        if include_current and current_version in version_list:
            previous.append(current_version)
            previous.sort(key=lambda x: Version(x), reverse=True)
        return previous[:num]

    def generate_sanitizer_version(self, version, head_sha):
        last_dash_index = version.rfind('-')
        if last_dash_index == -1:
            # TODO: fetch version with other method instead of hard code
            new_version = f"testing-{SANITIZER}-{ARCH}-{head_sha}"
            return new_version
        new_version = version[:last_dash_index + 1] + head_sha
        return new_version

    def get_latest_sanitizer_version(self, current_version, workflow_id, num=WEEK_PER_PAGE_NUM):
        version_id_list = []
        workflows = self.github_api.get_latest_workflow_run(workflow_id, num)
        if workflows is None:
            print("No latest workflow found")
            return version_id_list
        for workflow in workflows:
            id = workflow.get("id")
            name = workflow.get("name")
            status = workflow.get("status")
            conclusion = workflow.get("conclusion")
            head_sha = workflow.get("head_sha")
            created_at = workflow.get("created_at")
            if name is None or status is None or conclusion is None or head_sha is None or created_at is None:
                continue
            if status != "completed":
                print(f"workflow {id} still in progress, skip")
                continue
            print(f"Latest Completed Action Run: id: {id}, name: {name}, conclusion: {conclusion}, head_sha: {head_sha}, created_at: {created_at}")

            version = self.generate_sanitizer_version(current_version, head_sha)
            version_id_list.append(version)
        return version_id_list

    def get_previous_release(self, base_version=None, count=1):
        releases = self.github_api.get_latest_release(MAX_PER_PAGE_NUM)

        releases = [(r, self.tag_to_version(r["tag_name"])) for r in releases]
        releases = [(r, v) for r, v in releases if v is not None and self.is_release(v)]
        releases.sort(key=lambda x: x[1], reverse=True)

        if base_version:
            releases = [r for r, v in releases if Version(v) < Version(base_version)]
        else:
            releases = [r for r, _ in releases]
        return releases[:count]
