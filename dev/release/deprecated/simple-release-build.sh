#!/usr/bin/env bash
#-------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#-------------------------------------------------------------

function exit_with_usage {
  cat << EOF

simple-release-build.sh - A simplified version of the release scripts for producting versioned code/binary bundles

SYNOPSIS

usage: simple-release-build.sh [--overrideVersion=<version>] [--gitUrl=<repository-url]
                               [--gitCommitHash=<commit-or-branch-name>] [--force-download]

DESCRIPTION

This script omits all the bells and whistles to simply produce *-bin.tbz et al 

OPTIONS

-v=    --overrideVersion=<no-default>
         Specifies the version of the release
  
-u=    --gitUrl=https://github.com/apache/systemds.git
         The URL of the repository to clone
  
-g=    --gitCommitHash=master
         The tag, branch name or commit hash to check out

-f     --force-download
         Clone the repository again (to overwrite in consecutive runs)

-s     --skip-sign
         Skips signing with gnupg. Convenience option for internal use

-h     --help
         Call for help (print this text)
EXAMPLE

SYSTEMDS_ROOT=$(pwd) GNUPGHOME=<path-to-gnupg-dir> GPG_KEYID="<0xKeyID>" GPG_PASSPHRASE="<passphrase>"  dev/release/simple-release-build.sh --overrideVersion=1.2.3 -g=prep-release-0.2

<<
EOF
  exit 1
}

set -e

#if [ $# -eq 0 ]; then
#  exit_with_usage
#fi

# Process each provided argument configuration
while [ "${1+defined}" ]; do
  IFS="=" read -ra PARTS <<< "$1"
  case "${PARTS[0]}" in
    -g | --gitCommitHash)
      GIT_REF="${PARTS[1]}"
      shift
      ;;
    -v | --overrideVersion)
      RELEASE_VERSION="${PARTS[1]}"
      shift
      ;;
    -u | --gitUrl)
      GIT_URL="${PARTS[1]}"
      shift
      ;;
    -f | --force-download)
      FORCE_DL=1
      shift
      ;;
    -s | --skip-sign)
      SKIP_SIGN=1
      shift
      ;;
    *help* | -h)
      exit_with_usage
      exit 0
      ;;
    -*)
      echo "Error: Unknown option: $1" >&2
      exit 1
      ;;
    *)  # No more options
     break
     ;;
  esac
done

if [[ -z "$SYSTEMDS_ROOT" ]]; then
    echo
    echo "-------------------------------------------------------------"
    echo 'The environment variable SYSTEMDS_ROOT is not set. This'
    echo 'variable needs to point to the base of your SystemDS source'
    echo 'tree.'
    echo "-------------------------------------------------------------"

    exit_with_usage
fi

if [ -z ${SKIP_SIGN} ]; then
  if [[ -z "$GPG_PASSPHRASE" ]]; then
      echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
      echo 'unlock the GPG signing key that will be used to sign the release!'
      echo
      stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
  fi

  if [[ -z "$GPG_KEYID" ]]; then
      echo 'The environment variable GPG_KEYID is not set. Enter the key ID for the '
      echo ' GPG signing key that will be used to sign the release!'
      echo
      stty -echo && printf "GPG key ID: " && read GPG_KEYID && printf '\n' && stty echo
  fi
fi

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}
if [[ "$RELEASE_PUBLISH" == "true" && "$GIT_TAG" ]]; then
    GIT_REF="tags/$GIT_TAG"
fi

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}
if [[ -z "$GIT_URL" ]]; then
    echo "Using default URL"
    GIT_URL="https://github.com/apache/systemds.git"
fi

BASE_DIR=$(pwd)
RELEASE_WORK_DIR=$BASE_DIR/target/release

MVN="mvn --batch-mode --errors"

if [ -z ${SKIP_SIGN} ]; then
  PUBLISH_PROFILES="-Pdistribution,rat"
else
  PUBLISH_PROFILES="-Pdistribution,rat,skip-sign"
fi

RELEASE_STAGING_LOCATION="${SYSTEMDS_ROOT}/temp"

echo "  "
echo "-------------------------------------------------------------"
echo "------- Release preparation with the following parameters ---"
echo "-------------------------------------------------------------"
echo
echo "SYSTEMDS_ROOT       ==> $SYSTEMDS_ROOT"
echo "Git reference       ==> $GIT_REF"
if [ -z "$RELEASE_VERSION" ]; then
  echo "release version     ==> (from pom.xml)"
  else
  echo "release version     ==> $RELEASE_VERSION"
fi
echo "Deploying to        ==> $RELEASE_STAGING_LOCATION"
echo

function checkout_code {
    # Checkout code
    rm -rf "$RELEASE_WORK_DIR"
    mkdir -p "$RELEASE_WORK_DIR"
    cd "$RELEASE_WORK_DIR"
    git clone $GIT_URL
    cd systemds
    git checkout "$GIT_REF"
    git_hash=$(git rev-parse --short HEAD)
    echo "Checked out SystemDS git hash $git_hash"

    git clean -d -f -x

    cd "$BASE_DIR" #return to base dir
}

if [[ ! -d $RELEASE_WORK_DIR || FORCE_DL -eq 1 ]]; then
    echo "Cloning source repo..."
    checkout_code
fi

if [[ ! -d $RELEASE_STAGING_LOCATION ]]; then
  mkdir -p "$RELEASE_STAGING_LOCATION"
fi

TIMESTAMP=$(date +%Y-%m-%dT%H:%M:%S)
LOG_OUTPUT=$RELEASE_STAGING_LOCATION/simple-release-build-output-$TIMESTAMP.log

cd "$RELEASE_WORK_DIR"/systemds

if [[ -n $RELEASE_VERSION ]]; then
    echo "resetting version in pom.xml..." ; sleep 3
    $MVN versions:set -DnewVersion="$RELEASE_VERSION"
fi

if [ -z ${SKIP_SIGN} ]; then
  GPG_OPTS="-Dgpg.keyname=$GPG_KEYID -Dgpg.passphrase=$GPG_PASSPHRASE"
fi

# skipped mvn clean verify release:update-versions verify install:install deploy:deploy
#CMD="$MVN $PUBLISH_PROFILES deploy \
#  -DskiptTests \
#  -DaltDeploymentRepository=altDepRepo::default::file://$RELEASE_STAGING_LOCATION \
#  ${GPG_OPTS}"

CMD="$MVN $PUBLISH_PROFILES deploy \
  -DskiptTests \
  -DaltDeploymentRepository=altDepRepo::default::file:///temp \
  ${GPG_OPTS}"

echo "Executing: " "$CMD"

$CMD | tee "$LOG_OUTPUT"

cd "$BASE_DIR"
