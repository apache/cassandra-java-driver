#!/bin/bash -x

. ~/.jabba/jabba.sh
. ~/env.txt
cd $(dirname "$(readlink -f "$0")")/..
printenv | sort
# Install snapshot dependencies (e.g. native-protocol) that are not published to a public repo yet.
# Fail the build immediately if this does not succeed, and pass -U below so that any stale
# resolution-failure markers in the local repository do not block the freshly installed snapshot.
./install-snapshots.sh || exit 1
mvn -U -B -V install -DskipTests -Dmaven.javadoc.skip=true
jabba use ${TEST_JAVA_VERSION}
# Find out the latest patch version of Cassandra
PATCH_SERVER_VERSION=$(curl -s https://downloads.apache.org/cassandra/ | grep -oP '(?<=href=\")[0-9]+\.[0-9]+\.[0-9]+(?=)' | sort -rV | uniq -w 3 | grep $SERVER_VERSION)
printenv | sort
mvn -B -V verify -T 1 -Ptest-jdk-${TEST_JAVA_MAJOR_VERSION} -DtestJavaHome=$(jabba which ${TEST_JAVA_VERSION}) -Dccm.version=${PATCH_SERVER_VERSION} -Dccm.dse=false -Dmaven.test.failure.ignore=true -Dmaven.javadoc.skip=true
