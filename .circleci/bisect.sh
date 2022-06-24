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
INITIAL_DELAY_SECONDS=120
POLL_SECONDS=60

# Generate CircleCI config
sed -i.bak '/workflows:/d' $CIRCLE_CONFIG_FILE
sed -i.bak '/    version: 2/d' $CIRCLE_CONFIG_FILE
sed -i.bak '/java8_separate_tests/d' $CIRCLE_CONFIG_FILE
sed -i.bak '/java8_pre-commit_tests/d' $CIRCLE_CONFIG_FILE
sed -i.bak '/java11_separate_tests:/d' $CIRCLE_CONFIG_FILE
workflows="\
j8_repeated_tests_jobs: \&j8_repeated_tests_jobs\n\
  jobs:\n\
    - j8_build\n\
    - j8_repeated_utest:\n\
        requires:\n\
          - j8_build\n\
    - j8_repeated_dtest:\n\
        requires:\n\
          - j8_build\n\
    - j11_repeated_utest:\n\
        requires:\n\
          - j8_build\n\
    - j11_repeated_dtest:\n\
        requires:\n\
          - j8_build\n\
\n\
j11_repeated_tests_jobs: \&j11_repeated_tests_jobs\n\
  jobs:\n\
    - j11_build\n\
    - j11_repeated_utest:\n\
        requires:\n\
          - j11_build\n\
    - j11_repeated_dtest:\n\
        requires:\n\
          - j11_build\n\
\n\
workflows:\n\
    version: 2\n\
    java8_repeated_tests: \*j8_repeated_tests_jobs\n\
    java11_repeated_tests: \*j11_repeated_tests_jobs"
sed -i.bak "s/    java11_pre-commit_tests: \*j11_pre-commit_jobs/${workflows}/g" $CIRCLE_CONFIG_FILE
$CIRCLE_DIR/generate.sh "$@"

# Create a temporal testing branch, overriding it if it already exists
tested_commit=$(git rev-parse HEAD)
tested_branch=$(git rev-parse --abbrev-ref HEAD)
test_branch="${tested_branch}-bisect"
git update-ref -d refs/heads/$test_branch
git checkout -b $test_branch
echo Testing commit $tested_branch/$tested_commit on branch $test_branch

# Push CircleCI config to start workflow
git restore $CIRCLE_CONFIG_FILE
git add $CIRCLE_DIR/config.yml
git commit -m "DO NOT MERGE - CircleCI config for running repeated tests at $tested_branch/$tested_commit"
git push -f origin $test_branch
test_commit=$(git rev-parse HEAD)

# Go back to the original branch, removing the temporal testing branch
git checkout $tested_branch
git branch -D $test_branch

# Do some initial wait giving time to CircleCI to start the workflows
echo "Waiting $INITIAL_DELAY_SECONDS seconds for tests of $tested_branch/$tested_commit at $test_branch/$test_commit"
sleep $INITIAL_DELAY_SECONDS

# Poll GitHub status until the CircleCI run ether succeeds or fails
while true; do
  status=$(curl -s https://api.github.com/repos/adelapena/cassandra/commits/$test_commit/status | sed -n '2p')

  if echo $status | grep -q pending; then
    echo "Waiting $POLL_SECONDS seconds for tests of $tested_branch/$tested_commit at $test_branch/$test_commit"
    sleep $POLL_SECONDS
  elif echo $status | grep -q success; then
    echo "Tests for $tested_commit are successful"
    exit 0
  elif echo $status | grep -q failure; then
    echo "Tests for $tested_commit have failed"
    exit 1
  else
    echo "Unable to parse GitHub status info for commit $test_commit"
    exit -1
  fi
done
