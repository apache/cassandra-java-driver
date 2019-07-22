#!/bin/sh

# Install dependencies in the Travis build environment if they are snapshots.
# See .travis.yml

set -u

install_snapshot()
{
  URL=$1
  DIRECTORY_NAME=$2
  # Assume the snapshot we want is on the head of the default branch
  git clone --depth 1 ${URL} /tmp/${DIRECTORY_NAME}
  {
    cd /tmp/${DIRECTORY_NAME}
    mvn install -DskipTests
  }
}

grep -q '<native-protocol.version>.*-SNAPSHOT</native-protocol.version>' pom.xml && \
  install_snapshot https://github.com/datastax/native-protocol.git native-protocol
