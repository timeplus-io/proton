import logging
import sys
import argparse

from env_helper import PROTON_VERSION, PROTON_REPO, ARCH, REF
from version_helper import GitHubAPI, VersionHelper
from binary_size_check import get_complete_asset_sizes_by_version


if PROTON_VERSION is None:
    logging.error("PROTON_VERSION is None, could not find proton image")
    sys.exit(1)

if PROTON_REPO is None:
    logging.error("PROTON_REPO is None, could not find proton image")
    sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num', default=1, type=int, 
                        help='Number of base versions including specified versions and the latest versions that is automatically obtained')
    parser.add_argument('--versions', type=str, required=False, 
                        help='Specify baseline versions that must be tested. (e.g., 1.0.0,1.1.0)')
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    github_api = GitHubAPI()
    version_helper = VersionHelper()

    args = parse_args()
    specified_version = version_helper.get_version_list(args.versions)
    
    previous_version_list = version_helper.get_previous_release_version(PROTON_VERSION, args.num, True)
    version_list = version_helper.replace_version_list(previous_version_list, specified_version)

    # Skip versions with incomplete release assets
    valid_version_list = []
    for version in version_list:
        if get_complete_asset_sizes_by_version(version) is not None:
            valid_version_list.append(version)
            
    logging.info(f"valid version list: {valid_version_list}")
    
    with open("baseline_versions.txt", "w", encoding="utf-8") as f:
        f.write(str(valid_version_list))
