#!/bin/bash -x

. ~/.jabba/jabba.sh
. ~/env.txt
cd $(dirname "$(readlink -f "$0")")/..
printenv | sort
mvn -B -V install -DskipTests -Dmaven.javadoc.skip=true
jabba use ${TEST_JAVA_VERSION}
printenv | sort
mvn -B -V verify -T 1 -Ptest-jdk-${TEST_JAVA_MAJOR_VERSION} -DtestJavaHome=$(jabba which ${TEST_JAVA_VERSION}) -Dccm.version=${SERVER_VERSION} -Dccm.dse=false -Dmaven.test.failure.ignore=true -Dmaven.javadoc.skip=true
