#!/usr/bin/env bash

DRY_RUN=${DRY_RUN:-0}
ASF_REPO="https://github.com/apache/systemds"
ASF_REPO_CONTENT="https://raw.githubusercontent.com/apache/systemds"

# TODO: investigate this properly
# gpg: signing failed: Inappropriate ioctl for device
# https://github.com/j143/systemds/issues/75
GPG_TTY=$(tty)
export GPG_TTY

# Output font formatting
export TERM=ansi

bold() {
  tput bold
  echo -n "$@"

  # turn off bold
  tput sgr0
}

revcolor() {
  # standout mode
  tput smso
  echo -n "$@"
  tput rmso
}


# color values
# red 1; green 2; blue 4; magenta 5;
greencolor() {
  tput setaf 2
  echo -n "$@"
  tput sgr0
}

# exit with error message
error() {
  echo "$*"
  exit 1
}


# Read the configuration
read_config() {
  local PROMPT="$1"
  local DEFAULT="$2"
  
  local REPLY=

  read -p "$PROMPT [$DEFAULT]: " REPLY
  local RETVAL="${REPLY:-$DEFAULT}"
  if [ -z "$RETVAL" ]; then
    error "$PROMPT must be provided"
  fi
  echo "$RETVAL"
}


# parse version number from pom.xml
# <version> tag.
parse_version() {
  grep -e '<version>.*</version>' | \
    head -n 2 | tail -n 1 | cut -d '>' -f2 | cut -d '<' -f1
}

# function to log output to a .log file
run_silent() {
  local DESCRIPTION="$1"
  local LOG_FILE="$2"
  
  # Remove the first two arguments
  # https://ss64.com/bash/shift.html
  shift 2

  printf "\n =============== "
  printf "\n = $DESCRIPTION "
  printf "\n Executing command: "
  printf "\n $(bold $(greencolor $@ )) \n"
  printf "\n Log file: $LOG_FILE "
  printf "\n =============== \n"
  
  # 2>&1 https://stackoverflow.com/a/818284
  # 1 stdout, 2 stderr, >& redirect merger operator
  "$@" 1>"$LOG_FILE" 2>&1
  
  # a successful command returns 0 exit code
  local SUCCESS=$?
  if [ $SUCCESS != 0 ]; then
    printf "\n Command FAILED to Execute. Log files are available.\n"
    tail "$LOG_FILE"
    exit $SUCCESS
  fi
}

# TODO: git clone systemds function
# https://git-scm.com/docs/git-clean
# git clean -d -f -x

# check for the tag name in git repo
check_for_tag() {
    curl -s --head --fail "$ASF_REPO/releases/tag/$1" > /dev/null
}


# get the release info including
# branch details, snapshot version
# error validation
get_release_info() {
  if [ -z "$GIT_BRANCH" ]; then
    # If not branch is specified, find the latest branch from repo
    GIT_BRANCH=$(git ls-remote --heads "$ASF_REPO" |
      awk '{print $2}' |
      sort -r |
      head -n 1 |
      cut -d/ -f3)
  fi

  export GIT_BRANCH=$(read_config "Branch" "$GIT_BRANCH")

  # Find the current version for the branch
  local VERSION=$(curl -s "$ASF_REPO_CONTENT/$GIT_BRANCH/pom.xml" |
    parse_version)
  
  echo "Current branch version is $VERSION."

  if [[ ! $VERSION =~ .*-SNAPSHOT ]]; then
    error "Not a SNAPSHOT version: $VERSION"
  fi

  NEXT_VERSION="$VERSION"
  RELEASE_VERSION="${VERSION/-SNAPSHOT/}"
  SHORT_VERSION=$(echo "$VERSION" | cut -d . -f 1-2)
  local REV=$(echo "$RELEASE_VERSION" | cut -d . -f 3)

  # Find out what rc is being prepared.
  # - If the current version is "x.y.0", then this is rc1 of the "x.y.0" release.
  # - If not, need to check whether the previous version has been already released or not.
  #   - If it has, then we're building rc1 of the current version.
  #   - If it has not, we're building the next RC of the previous version.
  local RC_COUNT
  if [ $REV != 0 ]; then
    local PREV_REL_REV=$((REV - 1))
    local PREV_REL_TAG="v${SHORT_VERSION}.${PREV_REL_REV}"

    if check_for_tag "$PREV_REL_TAG"; then
      RC_COUNT=1
      REV=$((REV + 1))
      NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    else
      RELEASE_VERSION="${SHORT_VERSION}.${PREV_REL_REV}"
      RC_COUNT=$(git ls-remote --tags "$ASF_REPO" "v${RELEASE_VERSION}-rc*" | wc -l)
      RC_COUNT=$((RC_COUNT + 1))
    fi
  else
    REV=$((REV + 1))
    NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    RC_COUNT=1
  fi

  export NEXT_VERSION=$(read_config "Next development version" "$NEXT_VERSION")
  export RELEASE_VERSION=$(read_config "Release" "$RELEASE_VERSION")

  RC_COUNT=$(read_config "RC #" "$RC_COUNT")

  # Check if the RC already exists, and if re-creating the RC, skip tag
  # creation
  RELEASE_TAG="${RELEASE_VERSION}-rc${RC_COUNT}"
  SKIP_TAG=0

  if check_for_tag "$RELEASE_TAG"; then
    read -p "$RELEASE_TAG already exists. Continue anyway [Y/n]? " ANSWER
    if [ "$ANSWER" != "Y" ]; then
      error "Exiting."
    fi
    SKIP_TAG
  fi

  export RELEASE_TAG

  GIT_REF="$RELEASE_TAG"
  
  export GIT_REF
  export PACKAGE_VERSION="$RELEASE_TAG"

  # Git configuration info
  # The ASF ID is obtained from
  # https://people.apache.org/phonebook.html?unix=systemds
  if [ -z "$ASF_USERNAME" ]; then
    export ASF_USERNAME=$(read_config "ASF ID" "$LOGNAME")
  fi

  if [ -z "$GIT_NAME" ]; then
    GIT_NAME=$(git config user.name || echo "")
    export GIT_NAME=$(read_config "Full name" "$GIT_NAME")
  fi

  # git configuration info
  if [ -z "$GIT_EMAIL" ]; then
    export GIT_EMAIL="$ASF_USERNAME@apache.org"
  fi
  
  # GPG key configuration info
  if [ -z "$GPG_KEY" ]; then
    export GPG_KEY=$(read_config "GPG key" "$GIT_EMAIL")
  fi

  cat <<EOF
================
Release details:
BRANCH:     $GIT_BRANCH
VERSION:    $RELEASE_VERSION
TAG:        $RELEASE_TAG
NEXT:       $NEXT_VERSION
ASF ID:   $ASF_USERNAME
GPG KEY ID:    $GPG_KEY
FULL NAME:  $GIT_NAME
E-MAIL:     $GIT_EMAIL
================
EOF

#   read -p "Is this info correct [Y/n]? " ANSWER
  if [ -z "$CORRECT_RELEASE_INFO" ]; then
    CORRECT_RELEASE_INFO=$(read_config "Is the release info correct (1 for Yes, 0 for No) ?" "$CORRECT_RELEASE_INFO")
  fi
  
  if [[ ! "$CORRECT_RELEASE_INFO" = '1' ]]; then
    echo "Exiting."
    exit 1
  fi

  if [ -z "$ASF_PASSWORD" ]; then
    stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
  fi

  if [ -z "$GPG_PASSPHRASE" ]; then
    stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
  fi

  export ASF_PASSWORD
  export GPG_PASSPHRASE

}

is_dry_run() {
  # By default, evaluates to false
  [[ "$DRY_RUN" = 1 ]]
}

