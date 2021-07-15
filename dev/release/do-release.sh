#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-utils.sh"

# discussion on optional arguments
# https://stackoverflow.com/q/18414054
while getopts ":n" opt; do
  case $opt in
    n) DRY_RUN=1 ;;
    \?) error "Invalid option: $OPTARG" ;;
  esac
done

# Ask for release information
get_release_info


# tag
run_silent "Creating release tag $RELEASE_TAG..." "tag.log" \
    "$SELF/create-tag.sh"

# run_silent "Publish Release Candidates to the Nexus Repo..." "publish-snapshot.log" \
#     "$SELF/release-build.sh" publish-snapshot

git checkout $RELEASE_TAG
printf "\n checking out $RELEASE_TAG for building artifacts \n"

# NOTE:
# The following goals publishes the artifacts to
#  1) Nexus repo at repository.apache.org
#  2) SVN repo at dist.apache.org
# 
# are to be used together.

run_silent "Publish Release Candidates to the Nexus Repo..." "publish.log" \
    "$SELF/release-build.sh" publish-release


