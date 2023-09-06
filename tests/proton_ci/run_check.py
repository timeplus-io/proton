#!/usr/bin/env python
import sys
import logging
from typing import Tuple

from github import Github

from commit_status_helper import (
    format_description,
    post_commit_status,
)
from env_helper import GH_PERSONAL_ACCESS_TOKEN
from pr_info import PRInfo

TRUSTED_ORG_IDS = {
    87796806    # timeplus-io
}
# Individual trusted contirbutors who are not in any trusted organization.
# Can be changed in runtime: we will append users that we learned to be in
# a trusted org, to save GitHub API calls.
TRUSTED_CONTRIBUTORS = {
    e.lower()
    for e in [
    ]
}
PR_TEST_LABEL = 'pr-test'

def pr_is_by_trusted_user(pr_user_login, pr_user_orgs):
    if pr_user_login.lower() in TRUSTED_CONTRIBUTORS:
        logging.info("User '%s' is trusted", pr_user_login)
        return True

    logging.info("User '%s' is not trusted", pr_user_login)

    for org_id in pr_user_orgs:
        if org_id in TRUSTED_ORG_IDS:
            logging.info(
                "Org '%s' is trusted; will mark user %s as trusted",
                org_id,
                pr_user_login
            )
            return True
        logging.info("Org '%s' is not trusted", org_id)

    return False


# Returns whether we should look into individual checks for this PR. If not, it
# can be skipped entirely.
# Returns can_run, description, labels_state
def should_run_ci_for_pr(pr_info: PRInfo) -> Tuple[bool, str, str]:
    # Consider the labels and whether the user is trusted.
    print("Got labels", pr_info.labels)
    logging.info(f"pr_info.user_orgs: {pr_info.user_orgs}")
    if PR_TEST_LABEL not in pr_info.labels and not pr_is_by_trusted_user(
        pr_info.user_login, pr_info.user_orgs
    ):
        print(
            f"PRs by untrusted users need the '{PR_TEST_LABEL}' label - please contact a member of the core team"
        )
        return False, "Needs 'can be tested' label", "failure"

    return True, "No special conditions apply", "pending"


def main():
    logging.basicConfig(level=logging.INFO)
    pr_info = PRInfo(need_orgs=True, pr_event_from_api=True)

    # The case for special branches like backports and releases without created
    # PRs, like merged backport branches that are reset immediately after merge
    if pr_info.number == 0:
        print("::notice ::Cannot run, no PR exists for the commit")
        sys.exit(1)

    can_run, description, labels_state = should_run_ci_for_pr(pr_info)
    description = format_description(description)

    gh = Github(GH_PERSONAL_ACCESS_TOKEN, per_page=100)

    if not can_run:
        print("::notice ::Cannot run")
        post_commit_status(gh, pr_info.sha, "Labels check", description, labels_state, pr_info.pr_html_url)
        sys.exit(1)
    else:
        print("::notice ::Can run")
        post_commit_status(gh, pr_info.sha, "Labels check", description, labels_state, pr_info.pr_html_url)

if __name__ == '__main__':
    main()
