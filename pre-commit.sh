#!/usr/bin/env bash

# STASH_NAME="pre-commit-$(date +%s)"
# git stash save --keep-index $STASH_NAME

mvn clean test
RESULT=$?

# STASHES=$(git stash list)
# if [[ $STASHES == *$STASH_NAME* ]]; then
#   git stash pop
# fi

[ $RESULT -ne 0 ] && exit 1
exit 0
