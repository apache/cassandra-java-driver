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

mvn --projects core dependency:list -DincludeArtifactIds=native-protocol | \
  tee /dev/tty | \
  grep -q native-protocol.*SNAPSHOT
if [ $? -eq 0 ] ; then
  install_snapshot https://github.com/datastax/native-protocol.git native-protocol
fi
