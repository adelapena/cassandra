/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.util.Collections;
import java.util.Set;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * {@code WITH option1=... AND option2=...} options for SELECT statements.
 */
public class SelectOptions extends PropertyDefinitions
{
    public static final SelectOptions EMPTY = new SelectOptions();

    private static final Set<String> keywords = Collections.emptySet();

    /**
     * Validates all the {@code SELECT} options.
     *
     * @throws InvalidRequestException if any of the options are invalid
     */
    public void validate() throws RequestValidationException
    {
        validate(keywords, Collections.emptySet());
    }
}
