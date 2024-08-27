#!/usr/bin/python3

"""
This Python script allows you to list the
latest version numbers of Scylla and Cassandra.
You can specify whether you want the
versions of Scylla OSS or Scylla Enterprise,
either N latest stable X.Y.latest or
all non-obsolete RCs. You can also fetch
the latest version of Cassandra 3.
How are those versions fetched? We use Docker Hub
tags API.
"""

import requests
import argparse
import re
import json
import sys

DOCKER_HUB_TAGS_ENDPOINT = 'https://hub.docker.com/v2/namespaces/%s/repositories/%s/tags?page_size=1000'
DOCKER_HUB_SCYLLA_NAMESPACE = 'scylladb'

SCYLLA_OSS = (DOCKER_HUB_SCYLLA_NAMESPACE, 'scylla')
SCYLLA_ENTERPRISE = (DOCKER_HUB_SCYLLA_NAMESPACE, 'scylla-enterprise')

CASSANDRA_ENDPOINT = 'https://dlcdn.apache.org/cassandra/'
CASSANDRA_REGEX = re.compile(r'a href="([0-9])\.(\d+)\.(\d+)/"')

VERSION_DEFINITION_RE = re.compile(
    r'((?:(scylla-oss-stable):(\d+))|(?:(scylla-enterprise-stable):(\d+))|(?:(cassandra3-stable):(\d+))|(?:(cassandra4-stable):(\d+))|(?:(scylla-oss-rc))|(?:(scylla-enterprise-rc)))')
ONLY_DIGITS_RE = re.compile("^[0-9]+")

class Version:
    raw = ""
    invalid = False
    major = 0
    minor = 0
    minor_raw = ""
    minor_rest = ""
    patch = 0
    patch_raw = ""
    patch_rest = ""
    is_dev = False

    def __init__(self, ver):
        self.raw = ver
        chunks = ver.split(".", maxsplit=3)
        if len(chunks) < 3:
            self.invalid = True
            return

        self.major, self.minor_raw, self.patch_raw = chunks

        try:
            self.major = int(self.major)
        except Exception:
            self.invalid = True
            return

        digits = ONLY_DIGITS_RE.match(self.minor_raw)
        if digits:
            try:
                self.minor = int(self.minor_raw[digits.regs[0][0]:digits.regs[0][1]])
                self.minor_rest = self.minor_raw[digits.regs[0][1]:]
                if self.minor_rest:
                    self.is_dev = True
            except Exception:
                self.invalid = True
        else:
            self.minor = 0

        digits = ONLY_DIGITS_RE.match(self.patch_raw)
        if digits:
            try:
                self.patch = int(self.patch_raw[digits.regs[0][0]:digits.regs[0][1]])
                self.patch_rest = self.patch_raw[digits.regs[0][1]:]
                if self.patch_rest:
                    self.is_dev = True
            except Exception:
                self.invalid = True
        else:
            self.patch = 0

    def __str__(self):
        return self.raw

    def __repr__(self):
        return self.raw

    def __lt__(self, other):
        if self.major != other.major:
            return self.major < other.major
        if self.minor != other.minor:
            return self.minor < other.minor
        if self.patch != other.patch:
            return self.patch < other.patch
        if self.is_dev == other.is_dev:
            return False
        return self.is_dev


def fetch_docker_hub_tags(namespace, repository):
    tags = []

    # Fetch all pages of tags for a given repository
    current_page_endpoint = DOCKER_HUB_TAGS_ENDPOINT % (namespace, repository)
    while True:
        # Fetch a page
        tags_data = requests.get(current_page_endpoint).json()

        # Extract all tags from the response
        tags.extend(map(lambda e: e['name'], tags_data['results']))

        # Move to the next page if it's needed
        if tags_data['next'] is not None:
            current_page_endpoint = tags_data['next']
        else:
            break

    result = []
    for tag in tags:
        try:
            ver = Version(tag)
            if not ver.patch_raw or ver.invalid:
                continue
            result.append(ver)
        except Exception:
            continue

    return reversed(sorted(result))

rc_only = lambda x: x.is_dev
release_only = lambda x: not x.is_dev
scylla_oss_only = lambda x: x.major < 2000
scylla_enterprise_only = lambda x: x.major > 2000
cassandra_3_only = lambda x: x.major == 3
cassandra_4_only = lambda x: x.major == 4

def filter_versions(all_versions, count, *filters):
    result = []
    for ver in filter(lambda ver: all(map(lambda ft: ft(ver), filters)), all_versions):
        for existing_ver in result:
            if ver.major == existing_ver.major and ver.minor == existing_ver.minor:
                break
        else:
            if len(result) >= count:
                return result
            result.append(ver)
    return result

def fetch_all_cassandra_versions():
    # Download folder listing for Cassandra download site
    data = requests.get(CASSANDRA_ENDPOINT).text

    # Parse only those version numbers which match '3.NUM.NUM'
    # into tuple (3, NUM, NUM)
    data = CASSANDRA_REGEX.finditer(data)
    data = map(lambda e: e.groups(), data)
    data = map(lambda e: Version(f"{e[0]}.{e[1]}.{e[2]}"), data)
    return reversed(sorted(data))


if __name__ == '__main__':
    total_result = []

    def version_definition_type(arg):
        if not VERSION_DEFINITION_RE.match(arg):
            raise argparse.ArgumentTypeError(f"invalid version definition `{arg}`")
        return arg

    parser = argparse.ArgumentParser(description='Get scylla and cassandra versions.')
    parser.add_argument('definitions', metavar='VERSION-DEFINITION', type=version_definition_type, nargs='+',
                        help='A list of scylla and cassandra version definitions: scylla-oss-stable:<COUNT> | scylla-enterprise-stable:<COUNT> | cassandra3-stable:<COUNT> | cassandra4-stable:<COUNT> | scylla-oss-rc | scylla-enterprise-rc')

    parser.add_argument('--version-index', dest='version_index', type=int, default=0, help='print single version only')

    args = parser.parse_args()

    for version_def in args.definitions:
        groups = VERSION_DEFINITION_RE.match(version_def).groups()
        groups = [g for g in groups if g][1:]

        mode_name = groups[0]
        versions_count = int(groups[1]) if len(groups) > 1 else 0

        result = []
        if mode_name == 'scylla-oss-stable':
            result = filter_versions(fetch_docker_hub_tags(*SCYLLA_OSS), versions_count, scylla_oss_only, release_only)
        elif mode_name == 'scylla-enterprise-stable':
            result = filter_versions(fetch_docker_hub_tags(*SCYLLA_ENTERPRISE), versions_count, scylla_enterprise_only, release_only)
        elif mode_name == 'cassandra3-stable':
            result = filter_versions(fetch_all_cassandra_versions(), versions_count, cassandra_3_only)
        elif mode_name == 'cassandra4-stable':
            result = filter_versions(fetch_all_cassandra_versions(), versions_count, cassandra_4_only)
        elif mode_name == 'scylla-oss-rc':
            result = filter_versions(fetch_docker_hub_tags(*SCYLLA_OSS), versions_count, scylla_oss_only, rc_only)
        elif mode_name == 'scylla-enterprise-rc':
            result = filter_versions(fetch_docker_hub_tags(*SCYLLA_ENTERPRISE), versions_count, scylla_enterprise_only, rc_only)
        
        total_result += result

    total_result = [str(k) for k in reversed(sorted(total_result))]

    if args.version_index == 0:
        print(json.dumps(list(total_result)))
        sys.exit(0)
    if len(total_result) < args.version_index:
        print("No versions found", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(total_result[args.version_index-1]))
    sys.exit(0)
