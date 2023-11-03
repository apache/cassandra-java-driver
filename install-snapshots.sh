#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
