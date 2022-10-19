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

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;

/**
 * Generic, loose definition of a function parameter, able to infer the specific data type of the parameter in the
 * function specifically built by a {@link FunctionFactory} for a particular function call.
 */
public interface FunctionParameter
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

    /**
     * @param inferFromReceiver whether the parameter should try to use the function receiver to infer its data type
     * @return a function parameter definition that accepts columns of any data type
     */
    public static FunctionParameter anyType(boolean inferFromReceiver)
    {
        return new FunctionParameter()
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
