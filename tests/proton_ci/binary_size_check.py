import os
import sys
import logging
import argparse

from env_helper import PROTON_VERSION
from version_helper import GitHubAPI, VersionHelper


THRESHOLD = 0.05

BINARY_TO_CHECK = [
    "proton-{}-Linux-x86_64",
    "proton-{}-Linux-aarch64"
]

BINARY_TO_CHECK_DARWIN = [
    "proton-{}-Darwin-x86_64",
    "proton-{}-Darwin-arm64"
]

def get_asset_sizes(release):
    """ Get the asset sizes for a specified release """   
    assets = release.get("assets", [])
    asset_sizes = {asset["name"]: asset["size"] for asset in assets}
    return asset_sizes

def get_first_complete_asset_sizes(releases):
    """ Get the asset sizes for the first release with complete assets from multiple releases provided  """
    for release in releases:
        asset_sizes = get_asset_sizes(release)
        if all(asset.format(release['tag_name']) in asset_sizes for asset in BINARY_TO_CHECK):
            return release, asset_sizes
    return None, None

def get_complete_asset_sizes_by_version(version):
    github_api = GitHubAPI()
    version_helper = VersionHelper()

    tag = version_helper.version_to_tag(version)
    release = github_api.get_release_by_tag(tag)
    if release is None:
        print(f"release {tag} does not exist")
        return None

    asset_sizes = get_asset_sizes(release)
    if not all(binary.format(release['tag_name']) in asset_sizes for binary in BINARY_TO_CHECK):
        print(f"release assets incomplete in {version}")
        return None

    return asset_sizes

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=str, required=False, default='', help='Source version of binary size regression test')
    parser.add_argument('--target', type=str, required=False, default=PROTON_VERSION, help='Target version of binary size regression test')
    parser.add_argument('--threshold', type=float, required=False, default=THRESHOLD, help='Threshold of binary size regression test')
    parser.add_argument('--include-darwin', action='store_true', default=False, help='Include darwin in the binary size regression test')
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    
    if args.target == '':
        print(f"::error:: Target version is required")
        sys.exit(1)
    
    if args.include_darwin:
        BINARY_TO_CHECK += BINARY_TO_CHECK_DARWIN
    
    github_api = GitHubAPI()
    version_helper = VersionHelper()
    
    # Get target release asset sizes
    tag = version_helper.version_to_tag(args.target)
    release = github_api.get_release_by_tag(tag)
    if release is None:
        print(f"::error:: Target release {tag} does not exist")
        sys.exit(1)
    asset_sizes = get_asset_sizes(release)
    logging.info(f"target binary size: {asset_sizes}")
    if not all(binary.format(release['tag_name']) in asset_sizes for binary in BINARY_TO_CHECK):
        print(f"::error:: Target release assets incomplete in {args.target}")
        sys.exit(1)

    # Get source release asset sizes
    if args.source != '' and args.source != args.target:
        source_tag = version_helper.version_to_tag(args.source)
        source_release = github_api.get_release_by_tag(source_tag)
        if source_release is None:
            print(f"::error:: Source release {source_tag} does not exist")
            sys.exit(1)
        source_asset_sizes = get_asset_sizes(source_release)
        logging.info(f"source binary size: {source_asset_sizes}")
        if not all(binary.format(source_release['tag_name']) in source_asset_sizes for binary in BINARY_TO_CHECK):
            print(f"::error:: Source release assets incomplete in {args.source}")
            sys.exit(1)
    else:
        # Get the asset size of the last release with complete assets, going back up to 5 versions
        source_releases = version_helper.get_previous_release(base_version=args.target, count=5)
        if not source_releases:
            print(f"::error:: No complete released assets were found in the 5 versions prior to version {args.target}")
            sys.exit(1)
        source_release, source_asset_sizes = get_first_complete_asset_sizes(source_releases)
        logging.info(f"source binary size: {source_asset_sizes}")

    # Compare asset sizes between target release and source release
    logging.info(f"Comparing binary between {tag} and {source_release['tag_name']}:")

    exceed_threshold = False
    for binary in BINARY_TO_CHECK:
        binary_new= binary.format(tag)
        binary_old = binary.format(source_release['tag_name'])
        size_new = asset_sizes[binary_new] / (1024 * 1024)          # Convert to MB for better readability
        size_old = source_asset_sizes[binary_old] / (1024 * 1024)   # Convert to MB for better readability
        increase_ratio = (size_new - size_old) / size_old

        logging.info(f"{binary_new}: {size_old} → {size_new} MB ({increase_ratio*100:.2f}%)")

        if abs(increase_ratio) >= args.threshold:
            change_direction = "decreased" if increase_ratio < 0 else "increased"
            size_diff = abs(size_new - size_old)

            percent_change = abs(increase_ratio * 100)
            
            warning_message = f"Binary Size Change: '{binary_new}' {change_direction} by {percent_change:.2f}% | {source_release['tag_name']}: {size_old:.2f}MB → {tag}: {size_new:.2f}MB (Δ{size_diff:.2f}MB)"
            print(f"::warning::{warning_message}")
            exceed_threshold = True

    if exceed_threshold:
        logging.info(f"Binary size regression test between {tag} and {source_release['tag_name']}: FAILED")
        sys.exit(0)

    logging.info(f"Binary size regression test between {tag} and {source_release['tag_name']}: PASSED")
