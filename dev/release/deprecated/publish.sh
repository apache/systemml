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
RELEASE_TAG=v2.0
RELEASE_STAGING_LOCATION="/c/virtual\ D/SystemDS/systemds/temp"
BASE_DIR="/c/virtual\ D/SystemDS/systemds"
RELEASE_WORK_DIR="/c/virtual\ D/SystemDS/systemds/target/release2"
RELEASE_VERSION=2.0
RELEASE_RC=rc1
GIT_REF=-master
export GNUPGHOME="../../.gnupg_copy/" #relative path

function checkout_code {
    # Checkout code
    eval rm -rf $RELEASE_WORK_DIR
    eval mkdir -p $RELEASE_WORK_DIR
    eval cd $RELEASE_WORK_DIR
    git clone https://github.com/apache/systemds.git
    cd systemds
    git checkout $GIT_REF
    git_hash=`git rev-parse --short HEAD`
    echo "Checked out SystemDS git hash $git_hash"

    git clean -d -f -x
    #rm .gitignore
    #rm -rf .git

    eval cd "$BASE_DIR" #return to base dir
}


echo "Preparing release $RELEASE_VERSION"
# Checkout code
#checkout_code
eval cd $RELEASE_WORK_DIR/systemds

#Deploy to apache maven repo.
#settings.xml in maven home contains the username/passwd corresponding to ID apache.release.hattps
mvn -X -DaltDeploymentRepository=apache.releases.https::default::https://repository.apache.org/service/local/staging/deploy/maven2 \
clean package gpg:sign install:install deploy:deploy \
-DskiptTests -Darguments="-DskipTests -Dgpg.passphrase=\"$GPG_PASSPHRASE\"" -Dgpg.passphrase="$GPG_PASSPHRASE" $PUBLISH_PROFILES \

exit 0

