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

package org.apache.cassandra.cql3.functions.masking;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionParameter;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A {@link MaskingFunction} that replaces the specified column value by a certain replacement value.
 * <p>
 * The returned replacement value doesn't need to have the same type as the replaced value.
 * <p>
 * For example, given a text column named "username", {@code mask_replace(username, '****')} will always return
 * {@code ****}, independently of the actual value of that column.
 */
public class ReplaceMaskingFunction extends MaskingFunction
{
    public static final String NAME = "replace";

    private ReplaceMaskingFunction(FunctionName name, AbstractType<?> replacedType, AbstractType<?> replacementType)
    {
        super(name, replacementType, replacedType, replacementType);
    }

    @Override
    public final ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
    {
        return parameters.get(1);
    }

    /** @return a {@link FunctionFactory} to build new {@link ReplaceMaskingFunction}s. */
    public static FunctionFactory factory()
    {
        return new MaskingFunction.Factory(NAME, FunctionParameter.anyType(false), FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                AbstractType<?> replacedType = argTypes.get(0);
                AbstractType<?> replacementType = argTypes.get(1);

                return new ReplaceMaskingFunction(name, replacedType, replacementType);
            }
        };
    }
}
