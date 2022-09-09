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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Native functions for collections.
 */
public class CollectionFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(new FunctionFactory("map_keys", FunctionParameter.anyMap())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeMapKeysFunction(name().name, (MapType<?, ?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("map_values", FunctionParameter.anyMap())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeMapValuesFunction(name().name, (MapType<?, ?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_count", FunctionParameter.anyCollection())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionSizeFunction(name().name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_min", FunctionParameter.setOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionMinFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_max", FunctionParameter.setOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionMaxFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_sum", FunctionParameter.numericSetOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionSumFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });

        functions.add(new FunctionFactory("collection_avg", FunctionParameter.numericSetOrList())
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeCollectionAvgFunction(name.name, (CollectionType<?>) argTypes.get(0));
            }
        });
    }

    private static <K, V> NativeScalarFunction makeMapKeysFunction(String name, MapType<K, V> inputType)
    {
        SetType<K> outputType = SetType.getInstance(inputType.getKeysType(), false);

        return new NativeScalarFunction(name, outputType, inputType)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer value = parameters.get(0);
                if (value == null)
                    return null;

                Map<K, V> map = inputType.compose(value);
                Set<K> keys = map.keySet();
                return outputType.decompose(keys);
            }
        };
    }

    private static <K, V> NativeScalarFunction makeMapValuesFunction(String name, MapType<K, V> inputType)
    {
        ListType<V> outputType = ListType.getInstance(inputType.getValuesType(), false);

        return new NativeScalarFunction(name, outputType, inputType)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer value = parameters.get(0);
                if (value == null)
                    return null;

                Map<K, V> map = inputType.compose(value);
                List<V> values = ImmutableList.copyOf(map.values());
                return outputType.decompose(values);
            }
        };
    }

    private static <T> NativeScalarFunction makeCollectionSizeFunction(String name, CollectionType<T> inputType)
    {
        return new NativeScalarFunction(name, Int32Type.instance, inputType)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer value = parameters.get(0);
                if (value == null)
                    return null;

                int size = inputType.size(value);
                return Int32Type.instance.decompose(size);
            }
        };
    }

    private static <T> NativeScalarFunction makeCollectionMinFunction(String name, CollectionType<T> inputType)
    {
        AbstractType<?> elementsType = elementsType(inputType);
        return new NativeScalarFunction(name, elementsType, inputType)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer value = parameters.get(0);
                if (value == null)
                    return null;

                return inputType.min(value, protocolVersion);
            }
        };
    }

    private static <T> NativeScalarFunction makeCollectionMaxFunction(String name, CollectionType<T> inputType)
    {
        AbstractType<?> elementsType = elementsType(inputType);
        return new NativeScalarFunction(name, elementsType, inputType)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
            {
                ByteBuffer value = parameters.get(0);
                if (value == null)
                    return null;

                return inputType.max(value, protocolVersion);
            }
        };
    }

    public static AggregationFunction<?, ?> makeCollectionSumFunction(String name, CollectionType<?> inputType)
    {
        switch ((CQL3Type.Native) elementsType(inputType).asCQL3Type())
        {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return new AggregationFunction<>(
                        name, inputType, LongType.instance,
                        xs -> xs.stream().mapToLong(Number::longValue).sum());
            case FLOAT:
            case DOUBLE:
                return new AggregationFunction<>(
                        name, inputType, DoubleType.instance,
                        xs -> xs.stream().mapToDouble(Number::doubleValue).sum());
            case VARINT:
                return new AggregationFunction<BigInteger, BigInteger>(
                        name, inputType, IntegerType.instance,
                        xs -> xs.stream().reduce(BigInteger.ZERO, BigInteger::add));
            case DECIMAL:
                return new AggregationFunction<BigDecimal, BigDecimal>(
                        name, inputType, DecimalType.instance,
                        xs -> xs.stream().reduce(BigDecimal.ZERO, BigDecimal::add));
            default:
                throw new AssertionError("Expected numeric collection but found " + inputType);
        }
    }

    public static AggregationFunction<?, ?> makeCollectionAvgFunction(String name, CollectionType<?> inputType)
    {
        switch ((CQL3Type.Native) elementsType(inputType).asCQL3Type())
        {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return new AggregationFunction<>(
                        name, inputType, DoubleType.instance,
                        xs -> xs.stream().mapToLong(Number::longValue).average().orElse(Double.NaN));
            case FLOAT:
            case DOUBLE:
                return new AggregationFunction<>(
                        name, inputType, DoubleType.instance,
                        xs -> xs.stream().mapToDouble(Number::doubleValue).average().orElse(Double.NaN));
            case VARINT:
                return new AggregationFunction<BigInteger, BigInteger>(name, inputType, IntegerType.instance, xs -> {
                    if (xs.isEmpty())
                        return null; // There is no NaN for BigInteger

                    BigInteger sum = xs.stream().reduce(BigInteger.ZERO, BigInteger::add);
                    return sum.divide(BigInteger.valueOf(xs.size()));
                });
            case DECIMAL:
                return new AggregationFunction<BigDecimal, BigDecimal>(name, inputType, DecimalType.instance, xs -> {
                    if (xs.isEmpty())
                        return null; // There is no NaN for BigDecimal

                    BigDecimal sum = xs.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
                    return sum.divide(BigDecimal.valueOf(xs.size()), RoundingMode.HALF_EVEN);
                });
            default:
                throw new AssertionError("Expected numeric collection but found " + inputType);
        }
    }

    private static AbstractType<?> elementsType(CollectionType<?> type)
    {
        if (type.kind == CollectionType.Kind.LIST)
        {
            return ((ListType<?>) type).getElementsType();
        }
        else if (type.kind == CollectionType.Kind.SET)
        {
            return ((SetType<?>) type).getElementsType();
        }
        else
        {
            throw new AssertionError("Cannot get the element type of: " + type);
        }
    }

    private static class AggregationFunction<K extends Number, T extends Number> extends NativeScalarFunction
    {
        private final CollectionType<?> inputType;
        private final AbstractType<T> returnType;
        private final java.util.function.Function<Collection<K>, T> aggregator;

        public AggregationFunction(String name,
                                   CollectionType<?> inputType,
                                   AbstractType<T> returnType,
                                   java.util.function.Function<Collection<K>, T> aggregator)
        {
            super(name, returnType, inputType);
            this.inputType = inputType;
            this.returnType = returnType;
            this.aggregator = aggregator;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters)
        {
            ByteBuffer value = parameters.get(0);
            if (value == null)
                return null;

            Collection<K> xs = (Collection<K>) inputType.compose(value);
            T aggregated = aggregator.apply(xs);
            return returnType.decompose(aggregated);
        }
    }
}
