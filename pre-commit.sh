#!/usr/bin/env bash

git stash -q --keep-index
mvn clean test
RESULT=$?
git stash pop -q
[ $RESULT -ne 0 ] && exit 1
exit 0
