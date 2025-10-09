#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright 2018-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A script that calculates the release version number (based on the current Git
branch and/or recent tags in history) to assign to a tarball generated from the
current Git commit.

This script needs to remain compatible with its target platforms, which currently
includes RHEL 6, which uses Python 2.6!
"""

# XXX NOTE XXX NOTE XXX NOTE XXX
# After modifying this script it is advisable to execute the self-tests in this directory:
# - calc_release_version_selftest.sh
# - calc_release_version_selftest.py
# XXX NOTE XXX NOTE XXX NOTE XXX

# pyright: reportTypeCommentUsage=false

import datetime
import errno
import re
import subprocess
import optparse  # No 'argparse' on Python 2.6
import sys


class Version:
    def __init__(self, s):
        pat = r'(\d+)\.(\d+)\.(\d+)(\-\S+)?'
        match = re.match(pat, s)
        assert match, "Unrecognized version string %s" % s
        self.major, self.minor, self.micro = (
            map(int, (match.group(1), match.group(2), match.group(3))))

        if match.group(4):
            self.prerelease = match.group(4)[1:]
        else:
            self.prerelease = ''

    def __lt__(self, other):
        if self.major != other.major:
            return self.major < other.major
        if self.minor != other.minor:
            return self.minor < other.minor
        if self.micro != other.micro:
            return self.micro < other.micro
        if self.prerelease != other.prerelease:
            if self.prerelease != '' and other.prerelease == '':
                # Consider a prerelease less than non-prerelease.
                return True
            # For simplicity, compare prerelease versions lexicographically.
            return self.prerelease < other.prerelease

        # Versions are equal.
        return False

    def __eq__(self, other):
        self_tuple = self.major, self.minor, self.micro, self.prerelease
        other_tuple = other.major, other.minor, other.micro, other.prerelease
        return self_tuple == other_tuple


def parse_version(ver):
    return Version(ver)


parser = optparse.OptionParser(description=__doc__)
parser.add_option("--debug", "-d", action="store_true", help="Enable debug output")
parser.add_option("--previous", "-p", action="store_true", help="Calculate the previous version instead of the current")
parser.add_option("--next-minor", action="store_true", help="Calculate the next minor version instead of the current")
args, pos = parser.parse_args()
assert not pos, "No positional arguments are expected"


_DEBUG = args.debug  # type: bool


def debug(msg):  # type: (str) -> None
    if _DEBUG:
        sys.stderr.write(msg)
        sys.stderr.write("\n")
        sys.stderr.flush()


debug("Debugging output enabled.")

# This option indicates we are to determine the previous release version
PREVIOUS = args.previous  # type: bool
# This options indicates to output the next minor release version
NEXT_MINOR = args.next_minor  # type: bool

# fmt: off

PREVIOUS_TAG_RE = re.compile('(?P<ver>(?P<vermaj>[0-9]+)\\.(?P<vermin>[0-9]+)'
                             '\\.(?P<verpatch>[0-9]+)(?:-(?P<verpre>.*))?)')
RELEASE_TAG_RE = re.compile('(?P<ver>(?P<vermaj>[0-9]+)\\.(?P<vermin>[0-9]+)'
                            '\\.(?P<verpatch>[0-9]+)(?:-(?P<verpre>.*))?)')
RELEASE_BRANCH_RE = re.compile('(?:(?:refs/remotes/)?origin/)?(?P<brname>r'
                               '(?P<vermaj>[0-9]+)\\.(?P<vermin>[0-9]+))')


def check_output(args):  # type: (list[str]) -> str
    """
    Delegates to subprocess.check_output() if it is available, otherwise
    provides a reasonable facsimile.
    """
    debug('Run command: {0}'.format(args))
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE)
    except OSError as e:
        suppl = ''
        if e.errno == errno.ENOENT:
            suppl = 'Does the executable “{0}” not exist?'.format(args[0])
        raise RuntimeError("Failed to execute subprocess {0}: {1} [{2}]".format(args, e, suppl))
    out = proc.communicate()[0]
    ret = proc.poll()
    if ret:
        raise subprocess.CalledProcessError(ret, args[0])

    # git isn't guaranteed to always return UTF-8, but for our purposes
    # this should be fine as tags and hashes should be ASCII only.
    out = out.decode('utf-8')

    return out


def check_head_tag():  # type: () -> str | None
    """
    Checks the current HEAD to see if it has been tagged with a tag that matches
    the pattern for a release tag.  Returns release version calculated from the
    tag, or None if there is no matching tag associated with HEAD.
    """

    found_tag = False
    version_str = '0.0.0'
    version_parsed = parse_version(version_str)

    # have git tell us if any tags that look like release tags point at HEAD;
    # based on our policy, a commit should never have more than one release tag
    tags = check_output(['git', 'tag', '--points-at', 'HEAD', '--list', '1.*']).split()
    tag = ''
    if len(tags) == 1:
        tag = tags[0]
    elif len(tags) > 1:
        raise Exception('Expected 1 or 0 tags on HEAD, got: {}'.format(tags))

    release_tag_match = RELEASE_TAG_RE.match(tag)
    if release_tag_match:
        new_version_str = release_tag_match.group('ver')
        new_version_parsed = parse_version(new_version_str)
        if new_version_parsed > version_parsed: # type: ignore
            debug('HEAD release tag: ' + new_version_str)
            version_str = new_version_str
            version_parsed = new_version_parsed
            found_tag = True

    if found_tag:
        debug('Calculated version: ' + version_str)
        return version_str

    return None

def get_next_minor(prerelease_marker):  # type: (str) -> str
    """
    get_next_minor does the following:
        - Inspect the branches that fit the convention for a release branch.
        - Choose the latest and increment the minor version.
        - Append .0 to form the new version (e.g., r1.21 becomes 1.22.0)
        - Append a pre-release marker. (e.g. 1.22.0 becomes 1.22.0-20220201+gitf6e6a7025d)
    """

    version_str = '0.0.0'
    version_parsed = parse_version(version_str)

    version_new = {}
    # Use refs (not branches) to get local branches plus remote branches
    refs = check_output(['git', 'show-ref']).splitlines()
    for ref in refs:
        release_branch_match = RELEASE_BRANCH_RE.match(ref.split()[1])
        if release_branch_match:
            # Construct a candidate version from this branch name
            version_new['major'] = int(release_branch_match.group('vermaj'))
            version_new['minor'] = int(release_branch_match.group('vermin')) + 1
            version_new['patch'] = 0
            version_new['prerelease'] = prerelease_marker
            new_version_str = str(version_new['major']) + '.' + \
                              str(version_new['minor']) + '.' + \
                              str(version_new['patch']) + '-' + \
                              version_new['prerelease']
            new_version_parsed = parse_version(new_version_str)
            if new_version_parsed > version_parsed: # type: ignore
                version_str = new_version_str
                version_parsed = new_version_parsed
                debug('Found new best version "' + version_str \
                            + '" based on branch "' \
                            + release_branch_match.group('brname') + '"')
    return version_str

def get_branch_tags(active_branch_name):  # type: (str) -> list[str]
    """
    Returns a list of tags corresponding to the current branch, which must not
    be master.  If the specified branch is a release branch then return all tags
    based on the major/minor X.Y release version.  If the specified branch is
    neither master nor a release branch, then walk backwards in history until
    the first tag matching the glob '1.*' and return that tag.
    """

    if active_branch_name == 'master':
        raise Exception('this method is not meant to be called while on "master"')

    release_branch_match = RELEASE_BRANCH_RE.match(active_branch_name)
    if release_branch_match:
        # This is a release branch, so look for tags only on this branch
        tag_glob = release_branch_match.group('vermaj') + '.' \
                + release_branch_match.group('vermin') + '.*'
        return check_output(['git', 'tag', '--list', tag_glob]).splitlines()

    # Not a release branch, so look for the most recent tag in history
    commits = check_output(['git', 'log', '--pretty=format:%H', '--no-merges'])
    tags_by_obj = get_object_tags()
    for commit in commits.splitlines():
        got = tags_by_obj.get(commit)
        if got:
            return got
    # No tags
    return []


def iter_tag_lines():
    """
    Generate a list of pairs of strings, where the first is a commit hash, and
    the second is a tag that is associated with that commit. Duplicate commits
    are possible.
    """
    output = check_output(['git', 'tag', '--list', '1.*', '--format=%(*objectname)|%(tag)'])
    lines = output.splitlines()
    for l in lines:
        obj, tag = l.split('|', 1)
        if tag:
            yield obj, tag


def get_object_tags():  # type: () -> dict[str, list[str]]
    """
    Obtain a mapping between commit hashes and a list of tags that point to
    that commit. Untagged commits will not be included in the resulting map.
    """
    ret = {}  # type: dict[str, list[str]]
    for obj, tag in iter_tag_lines():
        ret.setdefault(obj, []).append(tag)
    return ret


def process_and_sort_tags(tags):  # type: (list[str]) -> list[str]
    """
    Given a string (as returned from get_branch_tags), return a sorted list of
    zero or more tags (sorted based on the Version comparison) which meet
    the following criteria:
        - a final release tag (i.e., 1.x.y without any pre-release suffix)
        - a pre-release tag which is not superseded by a release tag (i.e.,
          1.x.y-preX iff 1.x.y does not already exist)
    """

    processed_and_sorted_tags = []  # type: list[str]
    if not tags or len(tags) == 0:
        return processed_and_sorted_tags

    # find all the final release tags
    for tag in tags:
        release_tag_match = RELEASE_TAG_RE.match(tag)
        if release_tag_match and not release_tag_match.group('verpre'):
            processed_and_sorted_tags.append(tag)
    # collect together final release tags and pre-release tags for
    # versions that have not yet had a final release
    for tag in tags:
        tag_parts = tag.split('-')
        if len(tag_parts) >= 2 and tag_parts[0] not in processed_and_sorted_tags:
            processed_and_sorted_tags.append(tag)
    processed_and_sorted_tags.sort(key=Version)  # type: ignore

    return processed_and_sorted_tags

def main():
    """
    The algorithm is roughly:

        - Is the --next-minor flag passed? If "yes", then return the next minor
           release with a pre-release marker.
        - Is the current HEAD associated with a tag that looks like a release
           version?
        - If "yes" then use that as the version
        - If "no" then is the current branch master?
        - If "yes" the current branch is master, then return the next minor
           release with a pre-release marker.
        - If "no" the current branch is not master, then determine the most
           recent tag in history; strip any pre-release marker, increment the
           patch version, and append a new pre-release marker
    """

    version_str = '0.0.0'
    version_parsed = parse_version(version_str)
    head_commit_short = check_output(['git', 'rev-parse', '--revs-only',
                                      '--short=10', 'HEAD^{commit}']).strip()
    prerelease_marker = datetime.date.today().strftime('%Y%m%d') \
            + '+git' + head_commit_short

    if NEXT_MINOR:
        debug('Calculating next minor release')
        return get_next_minor(prerelease_marker)

    head_tag_ver = check_head_tag()
    if head_tag_ver:
        return head_tag_ver

    active_branch_name = check_output(['git', 'rev-parse',
                                       '--abbrev-ref', 'HEAD']).strip()
    debug('Calculating release version for branch: ' + active_branch_name)
    if active_branch_name == 'master':
        return get_next_minor(prerelease_marker)

    branch_tags = get_branch_tags(active_branch_name)
    tags = process_and_sort_tags(branch_tags)

    tag = tags[-1] if len(tags) > 0 else ''
    # at this point the RE match is redundant, but convenient for accessing
    # the components of the version string
    release_tag_match = RELEASE_TAG_RE.match(tag)
    if release_tag_match:
        version_new = {}
        version_new['major'] = int(release_tag_match.group('vermaj'))
        version_new['minor'] = int(release_tag_match.group('vermin'))
        version_new['patch'] = int(release_tag_match.group('verpatch')) + 1
        version_new['prerelease'] = prerelease_marker
        new_version_str = str(version_new['major']) + '.' + \
                          str(version_new['minor']) + '.' + \
                          str(version_new['patch']) + '-' + \
                          version_new['prerelease']
        new_version_parsed = parse_version(new_version_str)
        if new_version_parsed > version_parsed: # type: ignore
            version_str = new_version_str
            version_parsed = new_version_parsed
            debug('Found new best version "' + version_str \
                        + '" from tag "' + release_tag_match.group('ver') + '"')

    return version_str

def previous(rel_ver):  # type: (str) -> str
    """
    Given a release version, find the previous version based on the latest Git
    tag that is strictly a lower version than the given release version.
    """
    debug('Calculating previous release version (option -p was specified).')
    version_str = '0.0.0'
    version_parsed = parse_version(version_str)
    rel_ver_str = rel_ver
    rel_ver_parsed = parse_version(rel_ver_str)
    tags = check_output(['git', 'tag', '--list', '1.*']).splitlines()
    processed_and_sorted_tags = process_and_sort_tags(tags)
    for tag in processed_and_sorted_tags:
        previous_tag_match = PREVIOUS_TAG_RE.match(tag)
        if previous_tag_match:
            version_new = {}
            version_new['major'] = int(previous_tag_match.group('vermaj'))
            version_new['minor'] = int(previous_tag_match.group('vermin'))
            version_new['patch'] = int(previous_tag_match.group('verpatch'))
            version_new['prerelease'] = previous_tag_match.group('verpre')
            new_version_str = str(version_new['major']) + '.' + \
                              str(version_new['minor']) + '.' + \
                              str(version_new['patch'])
            if version_new['prerelease'] is not None:
                new_version_str += '-' + version_new['prerelease']
            new_version_parsed = parse_version(new_version_str)
            if new_version_parsed < rel_ver_parsed and new_version_parsed > version_parsed: # type: ignore
                version_str = new_version_str
                version_parsed = new_version_parsed
                debug('Found new best version "' + version_str \
                            + '" from tag "' + tag + '"')

    return version_str

if __name__ == "__main__":
    RELEASE_VER = previous(main()) if PREVIOUS else main()

    debug('Final calculated release version:')
    print(RELEASE_VER)
