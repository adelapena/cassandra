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

package org.apache.cassandra.cql3.functions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * A set of {@link FunctionFactories}.
 */
public class FunctionFactories implements Iterable<FunctionFactory>
{
    /** A set of function factories for native functions. */
    public static FunctionFactories instance = new FunctionFactories()
    {
        {
            AggregateFcts.addFactories(this);
        }
    };

    /** The registered function factories, associated to the name of the functions they build. */
    private final Map<FunctionName, FunctionFactory> factories = new HashMap<>();

    @Override
    public Iterator<FunctionFactory> iterator()
    {
        return factories.values().iterator();
    }

    /**
     * Adds the specified function factory to this set of factories.
     *
     * @param factory the function factory to be added
     */
    public void add(FunctionFactory factory)
    {
        factories.put(factory.name(), factory);
    }

    /**
     * Returns a function compatible with the provided signature if there is any factory able to create it.
     *
     * @param name the name of the function
     * @param keyspace the current keyspace
     * @param args the arguments in the function call
     * @param receiverType the expected return type of the function
     * @return a function applicable to the provided arguments if it is possible to create it
     */
    public Optional<Function> getFunction(FunctionName name,
                                          String keyspace,
                                          List<? extends AssignmentTestable> args,
                                          AbstractType<?> receiverType)
    {
        return Optional.ofNullable(factories.get(name.asNativeFunction()))
                       .map(factory -> factory.getOrCreateFunction(keyspace, args, receiverType));
    }
}
