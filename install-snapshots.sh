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

# Install snapshot dependencies that are not published to a public repository yet.
#
# The driver currently depends on native-protocol 1.5.3-SNAPSHOT (CEP-59 protocol types),
# which only exists on the branch behind datastax/native-protocol PR #61. Install it
# unconditionally: a probing "is the dependency a snapshot?" mvn run would write a
# resolution-failure marker into the local repository, which then blocks the main build
# even after the snapshot is installed.
#
# TODO: remove this script's invocation from ci/run-tests.sh (and revert this file to
# cloning https://github.com/datastax/native-protocol.git) once native-protocol 1.5.3
# is released.

set -eu

install_snapshot()
{
  URL=$1
  BRANCH=$2
  # Clone into a unique directory so concurrent builds on the same host cannot collide.
  CLONE_DIR=$(mktemp -d)/$(basename ${URL} .git)
  git clone --depth 1 --branch ${BRANCH} ${URL} ${CLONE_DIR}
  (
    cd ${CLONE_DIR}
    mvn -B install -DskipTests
  )
  rm -rf ${CLONE_DIR}
}

install_snapshot https://github.com/Shanzita/native-protocol.git cep-59
