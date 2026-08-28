#!/bin/bash -x

. ~/.jabba/jabba.sh
. ~/env.txt
cd $(dirname "$(readlink -f "$0")")/..
printenv | sort
mvn -B -V install -DskipTests -Dmaven.javadoc.skip=true
jabba use ${TEST_JAVA_VERSION}
# Find out the latest patch version of Cassandra
PATCH_SERVER_VERSION=$(curl -s https://downloads.apache.org/cassandra/ | grep -oP '(?<=href=\")[0-9]+\.[0-9]+\.[0-9]+(?=)' | sort -rV | uniq -w 3 | grep $SERVER_VERSION)
printenv | sort
mvn -B -V verify -T 1 -Ptest-jdk-${TEST_JAVA_MAJOR_VERSION} -DtestJavaHome=$(jabba which ${TEST_JAVA_VERSION}) -Dccm.version=${PATCH_SERVER_VERSION} -Dccm.dse=false -Dmaven.test.failure.ignore=true -Dmaven.javadoc.skip=true
