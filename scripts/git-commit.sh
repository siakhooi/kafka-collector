#!/bin/bash
set -e

# shellcheck disable=SC1091
. ./release.env

GIT_MESSAGE="$RELEASE_TITLE"

set -x
git commit -m "$GIT_MESSAGE"
