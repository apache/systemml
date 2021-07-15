#!/usr/bin/env bash
# Create release tags and version names

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-utils.sh"

# https://stackoverflow.com/q/59895
SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-utils.sh"

exit_with_usage() {
  local NAME=$(basename $0)
  cat << EOF
usage: $NAME

Tags a SystemDS release on a particular branch.
Inputs are specified with the following environment variables:
ASF_USERNAME - Apache Username
ASF_PASSWORD - Apache Password
GIT_NAME - Name to use with git
GIT_EMAIL - E-mail address to use with git
GIT_BRANCH - Git branch on which to make release
RELEASE_VERSION - Version used in pom files for release
RELEASE_TAG - Name of release tag
NEXT_VERSION - Development version after release
EOF
  exit 1
}

set -e
set -o pipefail

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

# docs related to stty 
# https://www.ibm.com/docs/en/aix/7.2?topic=s-stty-command
if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

for env in ASF_USERNAME ASF_PASSWORD RELEASE_VERSION RELEASE_TAG NEXT_VERSION GIT_EMAIL GIT_NAME GIT_BRANCH; do
  if [ -z "${!env}" ]; then
    echo "$env must be set to run this script"
    exit 1
  fi
done

uriencode() { jq -nSRr --arg v "$1" '$v|@uri'; }

declare -r ENCODED_ASF_PASSWORD=$(uriencode "$ASF_PASSWORD")

# git configuration
git config user.name "$GIT_NAME"
git config user.email "$GIT_EMAIL"

printf "$RELEASE_TAG \n"
printf "$RELEASE_VERSION\n"
printf "$NEXT_VERSION"

# options available at https://maven.apache.org/plugins/maven-gpg-plugin/sign-mojo.html
GPG_OPTS="-Dgpg.homedir=$GNUPGHOME -Dgpg.keyname=$GPG_KEY -Dgpg.passphrase=$GPG_PASSPHRASE"

printf "\n -Dgpg.homedir=$GNUPGHOME -Dgpg.keyname=$GPG_KEY -Dgpg.passphrase=$GPG_PASSPHRASE \n"

# Tag release version before `mvn release:prepare`
# tag python build
# PySpark version info we use dev0 instead of SNAPSHOT to be closer
# to PEP440.
# sed -i".tmp" 's/__version__ = .*$/__version__ = "'"$NEXT_VERSION.dev0"'"/' python/systemds/version.py

# change tags in docs
# docs/_config.yml
# update SYSTEMDS_VERSION
# sed -i 's/SYSTEMDS_VERSION:.*$/SYSTEMDS_VERSION: '"$RELEASE_VERSION"'/g' docs/_config.yml
# and run docs/updateAPI.sh to update version in api docs

dry_run=true

if [[ ! is_dry_run ]]; then
  dry_run=false
else
  dry_run=true
fi

printf "\n Dry Run?: $dry_run \n"

# NOTE:
# 
# When using gpg-plugin in conjunction with release plugin use 
# 
# $ mvn release:perform -Darguments=-Dgpg.passphrase=thephrase
#
# since the system properties of the current maven session are
# not propagated to the forked session automatically.
# 

CMD="mvn --batch-mode -DdryRun=false -Dtag=$RELEASE_TAG release:prepare \
                 -Dresume=false \
                 -DreleaseVersion=$RELEASE_VERSION \
                 -DdevelopmentVersion=$NEXT_VERSION \
                 -Dgpg.keyname=${GPG_KEY} -Dgpg.passphrase=${GPG_PASSPHRASE} \
                 -Darguments=${GPG_OPTS}"

printf "\n #### Executing command: #### \n"
printf "\n $(bold $(greencolor $CMD)) \n\n"

$CMD

# tag snapshot version after `mvn release:prepare`

# Change docs to dev snapshot tag
# sed -i".tmp1" 's/SYSTEMDS_VERSION:.*$/SYSTEMDS_VERSION: '"$NEXT_VERSION"'/g' docs/_config.yml
# and run docs/updateAPI.sh to update version in api docs
