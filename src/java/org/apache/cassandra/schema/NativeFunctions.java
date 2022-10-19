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

package org.apache.cassandra.schema;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * A container of native functions. It stores both pre-built function overloads ({@link NativeFunction}) and
 * dynamic generators of functions ({@link FunctionFactory}).
 */
public class NativeFunctions
{
    /** Pre-built function overloads. */
    private final Multimap<FunctionName, NativeFunction> functions = HashMultimap.create();

    /** Dynamic function factories. */
    private final Multimap<FunctionName, FunctionFactory> factories = HashMultimap.create();

    public void add(NativeFunction function)
    {
        functions.put(function.name(), function);
    }

    public void addAll(NativeFunction... functions)
    {
        for (NativeFunction function : functions)
            add(function);
    }

    public void add(FunctionFactory factory)
    {
        factories.put(factory.name(), factory);
    }

    /**
     * Returns all the registered pre-built functions overloads with the specified name.
     *
     * @param name a function name
     * @return the pre-built functions with the specified name
     */
    public Collection<NativeFunction> getFunctions(FunctionName name)
    {
        return functions.get(name);
    }

    /**
     * Returns all the registered functions factories with the specified name.
     *
     * @param name a function name
     * @return the function factories with the specified name
     */
    public Collection<FunctionFactory> getFactories(FunctionName name)
    {
        return factories.get(name);
    }

    /**
     * Returns the function with the specified name and exact signature if it exists, searching in both the pre-built
     * functions and the dynamic function factories.
     *
     * @param name the name of the searched function
     * @param argTypes the types of the function arguments
     * @return the function with the specified name and signature if it exists, {@link Optional#empty()} otherwise
     */
    public Optional<NativeFunction> find(FunctionName name, List<AbstractType<?>> argTypes)
    {
        Optional<NativeFunction> fun = functions.get(name).stream().filter(f -> f.typesMatch(argTypes)).findAny();

        if (fun.isPresent())
            return fun;

        return factories.get(name).stream().map(f -> f.getOrCreateFunction(argTypes, null, null, null)).findAny();
    }
}
