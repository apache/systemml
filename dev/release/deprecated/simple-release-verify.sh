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

simple-release-verify - Verifies the artifacts from a given directory.

SYNOPSIS

usage: simple-release-verify.sh [--compile | --verifyAll | verifyLic | verifyBin]

DESCRIPTION

Once artifacts are generated, this utility will verify the artifacts.

--compile
This will compile the utility source code which is not on regular source code path.

--verifyAll <--tag="Code based on Git tag will be validated."> [--workDir="Directory where output files will be created]
This will verify license, notice and binary files.

--verifyLic [--distDir="Directory Containing zip/tgz/tar.gz files]
This will verify license, notice in zip/tgz/tar.gz files.

--verifyBin <--tag="Code based on Git tag will be validated."> [--workDir="Directory where output files will be created]
This will verify binary distribution files for runtime correctness.

OPTIONS

--distDir            - Directory containing release artifacts (zip/tgz/tar.gz) files.

--workDir             - Directory where output files will be created.

EXAMPLES

# To compile utility source code
release-verify.sh --compile

# To verify release artifacts
release-verify.sh --verifyAll --tag=<tagName>
e.g. ./release-verify.sh --verifyAll --tag=v0.14.0-rc4

# To verify license and notices
release-verify.sh --verifyLic --distDir=<DistribLocation>
e.g. ./release-verify.sh --verifyLic --distDir=tmp/relValidation/downloads

# To verify binary files
release-verify.sh --verifyBin --tag=<tagName>
e.g. ./release-verify.sh --verifyBin --tag=v0.14.0-rc4


EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  echo "`date +%Y-%m-%dT%H:%M:%S`: ERROR: Insufficient parameters passed.";
  exit_with_usage
fi

# detect operating system to set correct directory separator
if [ "$OSTYPE" == "win32" ] ||  [ "$OSTYPE" == "msys" ] ; then
  echo "This script currently does not support Windows, as it makes use of symbolic linking via 'ln -s'"
  exit 1
fi

# Process each provided argument configuration
while [ "${1+defined}" ]; do
  IFS="=" read -ra PARTS <<< "$1"
  case "${PARTS[0]}" in
    --compile)
      COMPILE_CODE=true
      shift
      ;;
    --verifyAll)
      LIC_NOTICE_VERIFY=true
      BIN_VERIFY=true
      shift
      ;;
    --verifyLic)
      LIC_NOTICE_VERIFY=true
      shift
      ;;
    --verifyBin)
      BIN_VERIFY=true
      shift
      ;;
    --distDir)
      DIST_DIR="${PARTS[1]}"
      shift
      ;;
    --workDir)
      WORK_DIR="${PARTS[1]}"
      shift
      ;;
    --tag)
      TAG="${PARTS[1]}"
      shift
      ;;
    *help* | -h)
      exit_with_usage
     exit 0
     ;;
    -*)
     echo "`date +%Y-%m-%dT%H:%M:%S`: ERROR: Unknown option: $1" >&2
     exit_with_usage
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

ORIG_DIR=$(pwd)
EXEC_DIR="`dirname \"$0\"`"
if [[ ${EXEC_DIR:0:1} != "/" ]]; then
    EXEC_DIR=$ORIG_DIR/$EXEC_DIR
fi

if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR="$EXEC_DIR/tmp/relValidation"
elif [[ ${WORK_DIR:0:1} != "/" ]]; then
    WORK_DIR="$ORIG_DIR/$WORK_DIR"
fi

BUILD_TARGET_DIR="$EXEC_DIR/target"
if [[ ! -d $BUILD_TARGET_DIR ]]; then
  ln -s "$SYSTEMDS_ROOT"/target "$BUILD_TARGET_DIR"
fi

if [[ "$BIN_VERIFY" == "true" && -z "$TAG" ]]; then
    echo "`date +%Y-%m-%dT%H:%M:%S`: ERROR: Argument --tag is a mandatory variable for binary verification."
    exit_with_usage
fi

if [[ -n $COMPILE_CODE && -z "$DIST_DIR" ]]; then
  echo "`date +%Y-%m-%dT%H:%M:%S`: ERROR: Argument --distDir is missing (path to release (zip/tgz/tar.gz) files)"
  exit_with_usage
fi

cd $EXEC_DIR/src/test/java
if [[ "$COMPILE_CODE" == "true" ]]; then
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Compiling release verify utility..."

    javac -Xlint:unchecked -classpath ../../../target/release/systemds/target/lib/commons-compress-1.4.1.jar:../../../target/release/systemds/target/lib/commons-io-2.4.jar:. org/apache/sysds/validation/*.java

    cd "$ORIG_DIR" # Return to directory from it was called.
    exit 0
fi

if [[ "$BIN_VERIFY" == "true" ]]; then
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verifying binary files for runtime execution..."

    if [ -z $WORKING_DIR ] ; then
        WORKING_DIR="$EXEC_DIR/tmp/relValidation"
    fi

    rm -rf "$WORKING_DIR"/systemds
    mkdir -p "$WORKING_DIR"
    cd "$WORKING_DIR"

    ## Verify binary tgz files
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verifying binary tgz files..."
    rm -rf systemds-$TAG-bin
    tar -xzf $DIST_DIR/systemds-$TAG-bin.tgz
    cd systemds-$TAG-bin
    echo "print('hello world');" > hello.dml
    ./systemds hello.dml
    cd ..
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verification of tgz files completed successfully."

    ## Verify binary zip files
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verifying binary zip files..."
    rm -rf systemds-$TAG-bin
    unzip -q $DIST_DIR/systemds-$TAG-bin.zip
    cd systemds-$TAG-bin
    echo "print('hello world');" > hello.dml
    ./systemds hello.dml
    cd ..
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verification of zip files completed successfully."

    ## Verify src tgz files
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verifying source tgz files..."
    rm -rf systemds-$TAG-src
    tar -xzf $DIST_DIR/systemds-$TAG-src.tgz
    cd systemds-$TAG-src
    mvn clean package -P distribution -DskipTests
    cd target
    java -cp "./lib/*:SystemDS.jar" org.apache.sysds.api.DMLScript -s "print('hello world');"
    cd ../..
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verification of source archive completed successfully."

    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verification of all binary files for runtime execution completed successfully."
    echo "*********************************************************************************************************************"
fi

if [[ "$LIC_NOTICE_VERIFY" == "true" ]]; then
    echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verifying license and notices from zip/tgz/tar.gz files..."

    java -classpath ../../../target/release/systemds/target/lib/commons-compress-1.4.1.jar:../../../target/release/systemds/target/lib/commons-io-2.4.jar:. org/apache/sysds/validation/ValidateLicAndNotice $DIST_DIR
    RET_CODE=$?
    if [[ $RET_CODE == 0 ]]; then
       echo "`date +%Y-%m-%dT%H:%M:%S`: INFO: Verification of license and notices completed successfully."
    else
       echo "`date +%Y-%m-%dT%H:%M:%S`: ERROR: Verification of license and notices failed."
    fi
    echo "*********************************************************************************************************************"
fi

cd "$ORIG_DIR" # Return to directory from it was called.
exit $RET_CODE
