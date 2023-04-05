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

INITIAL_DELAY_SECONDS=120
POLL_SECONDS=60

CIRCLE_DIR=".circleci"
CIRCLE_CONFIG_FILE=$CIRCLE_DIR/config-2_1.yml

# exit if anything fails
set -e

die ()
{
  echo "ERROR: $*"
  print_help
  exit 1
}

print_help()
{
  echo "Usage: $0 [-c|-r|-b]"
  echo "   -c <config> The config.yml file for CircleCI"
  echo "   -r <remote> The name of the GitHub remote where the temporal changes will be force-pushed"
  echo "   -b <branch> The name of the temporal Git branch to be created"
}

# Parse arguments, most of them will be passed through to .circleci/generate.sh
config_args=""
while getopts "c:r:b:" opt; do
  case $opt in
      c ) config=$OPTARG;;
      r ) remote=$OPTARG;;
      b ) branch=$OPTARG;;
      \?) die "Invalid option: -$OPTARG";;
  esac
done
shift $((OPTIND-1))
if [ "$#" -ne 0 ]; then
    die "Unexpected config_args"
fi
if [ -z "$config" ]; then
  die "A config.yml file should be specified with -c option"
fi
if [ -z "$remote" ]; then
  die "A remote should be specified with -r option"
fi
if [ -z "$branch" ]; then
  die "A branch should be specified with -b option"
fi

# Ensure we don't accidentally use the main repo
remote_url=$(git config --get remote.${remote}.url)
if [[ ${remote_url} == *"apache/cassandra"* ]]; then
  die "The remote shouldn't be the Apache repo but a private one, $remote points to: $remote_url"
fi


# Create a temporal testing branch, overriding it if it already exists
tested_commit=$(git rev-parse HEAD)
tested_branch=$(git rev-parse --abbrev-ref HEAD)
git update-ref -d refs/heads/$branch
git checkout -b $branch
echo "Testing commit $tested_commit on branch $branch"

# Push CircleCI config to start workflow
cp "$config" $CIRCLE_DIR/config.yml
git add $CIRCLE_DIR/config.yml
git commit -m "DO NOT MERGE - Testing $tested_commit"
git push -f $remote $branch
commit=$(git rev-parse HEAD)
echo "Testing commit $tested_commit on commit $commit"

# Go back to the original commit, removing the temporal testing branch
git checkout $tested_commit
git branch -D $branch

# Prepare GitHub API URL
repo=`git config remote.${remote}.url | sed -e 's/.*://'` | sed -e 's/\.git//g'
url="https://api.github.com/repos/$repo/commits/$commit/status"

# Do some initial wait giving CircleCI time to start the workflows
echo "Waiting $INITIAL_DELAY_SECONDS seconds for tests of $tested_commit at $url"
sleep $INITIAL_DELAY_SECONDS

# Poll GitHub status until the CircleCI run ether succeeds or fails
while true; do
  status=$(curl -s ${url} | sed -n '2p')

  if echo $status | grep -q pending; then
    echo "Waiting $POLL_SECONDS seconds for tests of $tested_commit at $url"
    sleep $POLL_SECONDS
  elif echo $status | grep -q success; then
    echo "Tests for $tested_commit are successful"
    exit 0
  elif echo $status | grep -q failure; then
    echo "Tests for $tested_commit have failed"
    exit 1
  else
    echo "Unable to parse GitHub status info for commit $branch/$commit"
    exit -1
  fi
done
