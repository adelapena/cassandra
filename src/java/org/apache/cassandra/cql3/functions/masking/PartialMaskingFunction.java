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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.cql3.functions.FunctionFactory;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.FunctionParameter;
import org.apache.cassandra.cql3.functions.NativeFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.StringType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A {@link MaskingFunction} applied to the text representation of a column that, depending on {@link Type}:
 * <ul>
 * <li>Replaces each character between the supplied positions by the supplied padding character. In other words,
 * it will mask all the characters except the first m and last n.</li>
 * <li>Replaces each character before and after the supplied positions by the supplied padding character. In other
 * words, it will only mask all the first m and last n characters.</li>
 * </ul>
 * The returned value will allways be of type {@link org.apache.cassandra.db.marshal.StringType}, even if the masked
 * column has a different type.
 */
public class PartialMaskingFunction extends MaskingFunction
{
    /** The character to be used as padding if no other character is supplied when calling the function. */
    public static final char DEFAULT_PADDING_CHAR = '*';

    /** The type of partial masking to perform, inner or outer. */
    private final Type type;

    /** The original type of the masked column. */
    private final AbstractType<?> inputType;

    /** The type of the supplied padding character argument, {@code null} if that argument isn't supplied. */
    @Nullable
    private final StringType paddingArgumentType;

    private PartialMaskingFunction(FunctionName name,
                                   Type type,
                                   AbstractType<?> inputType,
                                   @Nullable StringType paddingArgumentType)
    {
        super(name, UTF8Type.instance, inputType, argumentsType(paddingArgumentType));

        this.type = type;
        this.inputType = inputType;
        this.paddingArgumentType = paddingArgumentType;
    }

    private static AbstractType<?>[] argumentsType(@Nullable StringType paddingArgumentType)
    {
        // The padding argument is optional, so we provide different signatures depending on whether it's present or not.
        // Also, the padding argument should be a single character, but we don't have a data type for that, so we use
        // a string-based argument. We will later validate on execution that the string argument is single-character.
        return paddingArgumentType == null
               ? new AbstractType<?>[]{ Int32Type.instance, Int32Type.instance }
               : new AbstractType<?>[]{ Int32Type.instance, Int32Type.instance, paddingArgumentType };
    }

    @Override
    public final ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
    {
        // Parse the beginning and end positions. No validation is needed since the masker accepts negatives,
        // but we should consider that the arguments migh be null.
        int begin = parameters.get(1) == null ? 0 : Int32Type.instance.compose(parameters.get(1));
        int end = parameters.get(2) == null ? 0 : Int32Type.instance.compose(parameters.get(2));

        // Parse the padding character. The type of the argument is a string of any length because we don't have a
        // character type in CQL, so we should verify that the passed string argument is single-character.
        char padding = DEFAULT_PADDING_CHAR;
        if (paddingArgumentType != null && parameters.get(3) != null)
        {
            String parameter = paddingArgumentType.compose(parameters.get(3));
            if (parameter.length() != 1)
            {
                throw new InvalidRequestException(String.format("The padding argument for function %s should " +
                                                                "be single-character, but '%s' has %d characters.",
                                                                name(), parameter, parameter.length()));
            }
            padding = parameter.charAt(0);
        }

        // Null column values aren't masked
        ByteBuffer value = parameters.get(0);
        if (value == null)
            return null;

        // We mask the string representation of the column value, even if the type of the column is not a string.
        String stringValue = toString(inputType, value);
        String maskedValue = type.mask(stringValue, begin, end, padding);
        return UTF8Type.instance.decompose(maskedValue);
    }

    private static <T> String toString(AbstractType<T> inputType, ByteBuffer value)
    {
        if (inputType.isTuple() || inputType.isUDT())
            return inputType.toCQLString(value);

        TypeSerializer<T> serializer = inputType.getSerializer();
        return serializer.toString(serializer.deserialize(value));
    }

    public enum Type
    {
        /** Masks everything except the first {@code begin} and last {@code end} characters. */
        INNER
        {
            @Override
            protected boolean shouldMask(int pos, int begin, int end)
            {
                return pos >= begin && pos <= end;
            }
        },
        /** Masks only the first {@code begin} and last {@code end} characters. */
        OUTER
        {
            @Override
            protected boolean shouldMask(int pos, int begin, int end)
            {
                return pos < begin || pos > end;
            }
        };

        protected abstract boolean shouldMask(int pos, int begin, int end);

        @VisibleForTesting
        public String mask(String value, int begin, int end, char padding)
        {
            if (StringUtils.isEmpty(value))
                return value;

            int size = value.length();
            int endIndex = size - 1 - end;
            char[] chars = new char[size];

            for (int i = 0; i < size; i++)
            {
                chars[i] = shouldMask(i, begin, endIndex) ? padding : value.charAt(i);
            }

            return new String(chars);
        }
    }

    /** @return a collection of function factories to build new {@code PartialMaskingFunction} functions. */
    public static Collection<FunctionFactory> factories()
    {
        return Stream.of(Type.values())
                     .map(PartialMaskingFunction::factory)
                     .collect(Collectors.toSet());
    }

    private static FunctionFactory factory(Type type)
    {
        return new MaskingFunction.Factory(type.name(),
                                           FunctionParameter.anyType(false),
                                           FunctionParameter.fixed(Int32Type.instance),
                                           FunctionParameter.fixed(Int32Type.instance),
                                           FunctionParameter.optional(FunctionParameter.fixed(UTF8Type.instance)))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                switch (argTypes.size())
                {
                    case 3:
                        return new PartialMaskingFunction(name, type, argTypes.get(0), null);
                    case 4:
                        return new PartialMaskingFunction(name, type, argTypes.get(0), UTF8Type.instance);
                    default:
                        throw invalidNumberOfArgumentsException();
                }
            }
        };
    }
}
