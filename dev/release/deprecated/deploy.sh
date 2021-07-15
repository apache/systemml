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
GPG_KEYID=$1
GPG_PASSPHRASE=$2
DEVELOPMENT_VERSION=2.1.0-SNAPSHOT
RELEASE_STAGING_LOCATION="/c/virtual\ D/SystemDS/systemds/temp"
BASE_DIR="/c/virtual\ D/SystemDS/systemds"
#BASE_DIR="../.." #points to systemds directory
RELEASE_WORK_DIR=$BASE_DIR/target/release2
RELEASE_VERSION=2.0.0
RELEASE_RC=rc1
GIT_REF=-master
#export GNUPGHOME="/c/virtual\ D/SystemDS/systemds/target/.gnupg_copy"
export GNUPGHOME="../../../target/.gnupg_copy/"

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

# Pull the latest code (with committed pom changes) and deploy to the local target directory
checkout_code
# Remove SNAPSHOT from the version in pom
eval cd $RELEASE_WORK_DIR/systemds
#sed -i "s/<version>$RELEASE_VERSION-SNAPSHOT<\/version>/<version>$RELEASE_VERSION<\/version>/" pom.xml
sed -i "s/<version>$DEVELOPMENT_VERSION<\/version>/<version>$RELEASE_VERSION<\/version>/" pom.xml
GPG_OPTS="-Dgpg.keyname=$GPG_KEYID -Dgpg.passphrase=$GPG_PASSPHRASE"
# Deploy to /target folder for the next job to pick the artifacts up for there
CMD="$MVN $PUBLISH_PROFILES deploy \
-DskiptTests \
-DaltDeploymentRepository=altDepRepo::default::file:./target \
${GPG_OPTS}"

echo "Executing: " "$CMD"
$CMD

eval cd $RELEASE_WORK_DIR


exit 0

