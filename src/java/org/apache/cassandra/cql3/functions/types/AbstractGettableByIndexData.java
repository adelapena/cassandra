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
package org.apache.cassandra.cql3.functions.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.reflect.TypeToken;

import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;
import org.apache.cassandra.transport.ProtocolVersion;

abstract class AbstractGettableByIndexData implements GettableByIndexData
{

    protected final ProtocolVersion protocolVersion;

    AbstractGettableByIndexData(ProtocolVersion protocolVersion)
    {
        this.protocolVersion = protocolVersion;
    }

    /**
     * Returns the type for the value at index {@code i}.
     *
     * @param i the index of the type to fetch.
     * @return the type of the value at index {@code i}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract DataType getType(int i);

    /**
     * Returns the name corresponding to the value at index {@code i}.
     *
     * @param i the index of the name to fetch.
     * @return the name corresponding to the value at index {@code i}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract String getName(int i);

    /**
     * Returns the value at index {@code i}.
     *
     * @param i the index to fetch.
     * @return the value at index {@code i}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract ByteBuffer getValue(int i);

    protected abstract CodecRegistry getCodecRegistry();

    protected <T> TypeCodec<T, ?> codecFor(int i)
    {
        return getCodecRegistry().codecFor(getType(i));
    }

    protected <T> TypeCodec<T, ?> codecFor(int i, Class<T> javaClass)
    {
        return getCodecRegistry().codecFor(getType(i), javaClass);
    }

    @SuppressWarnings("UnstableApiUsage")
    protected <T> TypeCodec<T, ?> codecFor(int i, TypeToken<T> javaType)
    {
        return getCodecRegistry().codecFor(getType(i), javaType);
    }

    protected <T> TypeCodec<T, ?> codecFor(int i, T value)
    {
        return getCodecRegistry().codecFor(getType(i), value);
    }

    void checkType(int i, DataType.Name actual)
    {
        DataType.Name expected = getType(i).getName();
        if (!actual.isCompatibleWith(expected))
            throw new InvalidTypeException(
            String.format("Value %s is of type %s, not %s", getName(i), expected, actual));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(int i)
    {
        return getValue(i) == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBool(int i)
    {
        return !isNull(i) && get(i, Boolean.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(int i)
    {
        return isNull(i) ? 0 : get(i, Byte.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getShort(int i)
    {
        return isNull(i) ? 0 : get(i, Short.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(int i)
    {
        return isNull(i) ? 0 : get(i, Integer.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(int i)
    {
        return isNull(i) ? 0 : get(i, Long.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getTimestamp(int i)
    {
        return get(i, Date.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate getDate(int i)
    {
        return get(i, LocalDate.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTime(int i)
    {
        return get(i, Long.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(int i)
    {
        return isNull(i) ? 0 : get(i, Float.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(int i)
    {
        return isNull(i) ? 0 : get(i, Double.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytesUnsafe(int i)
    {
        ByteBuffer value = getValue(i);
        return value == null ? null : value.duplicate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytes(int i)
    {
        return get(i, ByteBuffer.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(int i)
    {
        return get(i, String.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getVarint(int i)
    {
        return get(i, BigInteger.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getDecimal(int i)
    {
        return get(i, BigDecimal.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getUUID(int i)
    {
        return get(i, UUID.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getInet(int i)
    {
        return get(i, InetAddress.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("UnstableApiUsage")
    public <T> List<T> getList(int i, Class<T> elementsClass)
    {
        return getList(i, TypeToken.of(elementsClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("UnstableApiUsage")
    public <T> List<T> getList(int i, TypeToken<T> elementsType)
    {
        ByteBuffer value = getValue(i);
        TypeToken<List<T>> javaType = TypeTokens.listOf(elementsType);
        return codecFor(i, javaType).deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("UnstableApiUsage")
    public <T> Set<T> getSet(int i, Class<T> elementsClass)
    {
        return getSet(i, TypeToken.of(elementsClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("UnstableApiUsage")
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType)
    {
        ByteBuffer value = getValue(i);
        TypeToken<Set<T>> javaType = TypeTokens.setOf(elementsType);
        return codecFor(i, javaType).deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("UnstableApiUsage")
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass)
    {
        return getMap(i, TypeToken.of(keysClass), TypeToken.of(valuesClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("UnstableApiUsage")
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType)
    {
        ByteBuffer value = getValue(i);
        TypeToken<Map<K, V>> javaType = TypeTokens.mapOf(keysType, valuesType);
        return codecFor(i, javaType).deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getVector(int i)
    {
        return get(i, List.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UDTValue getUDTValue(int i)
    {
        return get(i, UDTValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleValue getTupleValue(int i)
    {
        return get(i, TupleValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(int i)
    {
        return get(i, codecFor(i));
    }

    @Override
    public <T> T get(int i, Class<T> targetClass)
    {
        return get(i, codecFor(i, targetClass));
    }

    @Override
    @SuppressWarnings("UnstableApiUsage")
    public <T> T get(int i, TypeToken<T> targetType)
    {
        return get(i, codecFor(i, targetType));
    }

    @Override
    public <T> T get(int i, TypeCodec<T, ?> codec)
    {
        checkType(i, codec.getCqlType().getName());
        ByteBuffer value = getValue(i);
        return codec.deserialize(value);
    }
}
