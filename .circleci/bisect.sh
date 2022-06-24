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

# setup the workflows for repeated tests only
sed -i.bak '/java8_separate_tests/s/^/#/' $CIRCLE_CONFIG_FILE
sed -i.bak '/java8_pre-commit_tests/s/^/#/' $CIRCLE_CONFIG_FILE
sed -i.bak '/java8_repeated_tests/s/^#//' $CIRCLE_CONFIG_FILE
sed -i.bak '/java11_repeated_tests/s/^#//' $CIRCLE_CONFIG_FILE
sed -i.bak '/java11_separate_tests/s/^/#/' $CIRCLE_CONFIG_FILE
sed -i.bak '/java11_pre-commit_tests/s/^/#/' $CIRCLE_CONFIG_FILE
