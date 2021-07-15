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

MVN=mvn
PUBLISH_PROFILES="-Pdistribution,rat"
DRY_RUN=-DdryRun=true
GPG_PASSPHRASE=$1
DEVELOPMENT_VERSION=2.1.0-SNAPSHOT
RELEASE_TAG=v2.0
RELEASE_STAGING_LOCATION="/c/virtual\ D/SystemDS/systemds/temp"
BASE_DIR="/c/virtual\ D/SystemDS/systemds"
RELEASE_WORK_DIR="/c/virtual\ D/SystemDS/systemds/target/release2"
RELEASE_VERSION=2.0.0
RELEASE_RC=rc1
GIT_REF=-master
#export GNUPGHOME="/c/virtual\ D/SystemDS/systemds/target/.gnupg_copy"
export GNUPGHOME="../../../target/.gnupg_copy/"

RELEASE_STAGING_REMOTE="https://dist.apache.org/repos/dist/dev/systemds/"
eval cd $RELEASE_STAGING_LOCATION;
rm -rf svn-release-staging
# Checkout the artifacts
svn co $RELEASE_STAGING_REMOTE svn-release-staging
rm -rf svn-release-staging/$RELEASE_VERSION-$RELEASE_RC
# Create a new folder for this release
mkdir -p svn-release-staging/$RELEASE_VERSION-$RELEASE_RC
# Copy the artifacts from target
eval cp $RELEASE_WORK_DIR/systemds/target/systemds-*-bin.* svn-release-staging/$RELEASE_VERSION-$RELEASE_RC/
eval cp $RELEASE_WORK_DIR/systemds/target/systemds-*-src.* svn-release-staging/$RELEASE_VERSION-$RELEASE_RC/

cd svn-release-staging/$RELEASE_VERSION-$RELEASE_RC/
rm -f *.asc
for i in *.zip *.tgz; do gpg --output $i.asc --detach-sig --armor $i; done
rm -f *.sha512
for i in *.zip *.tgz; do shasum -a 512 $i > $i.sha512; done

cd .. #exit $RELEASE_VERSION-$RELEASE_RC/

#svn add $RELEASE_VERSION-$RELEASE_RC/
svn add $(svn status | awk '{$1=""; print $0}')
#svn ci -m"Apache systemds $RELEASE_VERSION-$RELEASE_RC"
#manually commit from tortoise

exit 0

