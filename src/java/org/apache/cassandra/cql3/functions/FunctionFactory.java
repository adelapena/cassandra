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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Class for dynamically building different overloads of a CQL {@link Function} according to specific function calls.
 * <p>
 * For example, the factory for the {@code max} function will return a {@code (text) -> text} function if it's called
 * with an {@code text} argument, like in {@code max('abc')}. It however will return a {@code (list<int>) -> list<int>}
 * function if it's called with {@code max([1,2,3])}, etc.
 * <p>
 * This is meant to be used to create functions that require too many overloads to have them pre-created in memory. Note
 * that in the case of functions accepting collections, tuples or UDTs the number of overloads is potentially infinite.
 */
public abstract class FunctionFactory
{
    /** The name of the built functions. */
    protected final FunctionName name;

    /** The accepted parameters. */
    protected final List<Parameter> parameters;

    /**
     * @param name the name of the built functions
     * @param parameters the accepted parameters
     */
    public FunctionFactory(String name, Parameter... parameters)
    {
        this.name = FunctionName.nativeFunction(name);
        this.parameters = Arrays.asList(parameters);
    }

    public FunctionName name()
    {
        return name;
    }

    /**
     * Returns a function with a signature compatible with the specified function call.
     *
     * @param keyspace the current keyspace
     * @param args the arguments in the function call for which the function is going to be built
     * @param receiverType the expected return type of the function call for which the function is going to be built
     * @return a function with a signature compatible with the specified function call
     */
    public Function getOrCreateFunction(String keyspace, List<? extends AssignmentTestable> args, AbstractType<?> receiverType)
    {
        // validate the number of arguments
        if (args.size() != parameters.size())
            throw new InvalidRequestException("Invalid number of arguments for function " + this);

        // try to infer the types of the arguments
        List<AbstractType<?>> types = new ArrayList<>(args.size());
        for (int i = 0; i < args.size(); i++)
        {
            AssignmentTestable arg = args.get(i);
            AbstractType<?> type = parameters.get(i).inferType(keyspace, arg, receiverType);
            if (type == null)
                throw new InvalidRequestException("Cannot infer type for argument " + arg);
            types.add(type);
        }

        return getOrCreateFunction(types, receiverType);
    }

    /**
     * Returns a function compatible with the specified signature.
     *
     * @param argTypes the types of the function arguments
     * @param receiverType the expected return type of the function
     * @return a function compatible with the specified signature
     */
    protected abstract Function getOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType);

    @Override
    public String toString()
    {
        return String.format("%s(%s)", name, parameters.stream().map(Object::toString).collect(Collectors.joining(", ")));
    }

    /**
     * Generic definition of a function parameter, able to infer the data type of the parameter in the function
     * specifically built for a particular function call.
     */
    public interface Parameter
    {
        /**
         * Tries to infer the data type of the parameter for an argument in a call to the function.
         *
         * @param keyspace the current keyspace
         * @param arg a parameter value in a specific function call
         * @param receiverType the type of the object that will receive the result of the function call
         * @return the inferred data type of the parameter, or {@link null} it isn't possible to infer it
         */
        @Nullable
        AbstractType<?> inferType(String keyspace, AssignmentTestable arg, @Nullable AbstractType<?> receiverType);
    }

    /**
     * @param inferFromReceiver whether the parameter should try to use the function receiver to infer its data type
     * @return a function parameter definition that accepts columns of any data type
     */
    public static Parameter anyType(boolean inferFromReceiver)
    {
        return new Parameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace, AssignmentTestable arg, AbstractType<?> receiverType)
            {
                AbstractType<?> type = arg.getCompatibleTypeIfKnown(keyspace, receiverType);

                if (type == null)
                    type = inferFromReceiver ? receiverType : BytesType.instance;

                return type;
            }

            @Override
            public String toString()
            {
                return "any";
            }
        };
    }
}
