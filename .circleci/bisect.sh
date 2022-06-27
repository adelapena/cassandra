#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CIRCLE_DIR=`dirname $0`
CIRCLE_CONFIG_FILE=$CIRCLE_DIR/config-2_1.yml
CASSANDRA_DIR="$(dirname "$CIRCLE_DIR")"

die ()
{
  echo "ERROR: $*"
  print_help
  exit 1
}

tested_commit=$(git rev-parse HEAD)
tested_branch=$(git rev-parse --abbrev-ref HEAD)
echo Testing commit $tested_commit on branch @tested_branch

$CIRCLE_DIR/generate.sh -r "$@"
git add $CIRCLE_DIR/config.yml
git commit -m "DO NOT MERGE - CircleCI testing $tested_commit"
git push origin $tested_branch
test_commit=$(git rev-parse HEAD)

sleep 20
status=$(curl https://api.github.com/repos/adelapena/cassandra/commits/$test_commit/status | sed -n '2p')

if echo $status | grep -q success; then
  echo success
elif echo $status | grep -q failure; then
  echo failure
elif echo $status | grep -q pending; then
  echo pending
fi
