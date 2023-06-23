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
import java.util.*;

import com.google.common.reflect.TypeToken;

import org.apache.cassandra.transport.ProtocolVersion;

abstract class AbstractAddressableByIndexData<T extends SettableByIndexData<T>>
extends AbstractGettableByIndexData implements SettableByIndexData<T>
{
    final ByteBuffer[] values;

    AbstractAddressableByIndexData(ProtocolVersion protocolVersion, int size)
    {
        super(protocolVersion);
        this.values = new ByteBuffer[size];
    }

    @SuppressWarnings("unchecked")
    T setValue(int i, ByteBuffer value)
    {
        values[i] = value;
        return (T) this;
    }

    @Override
    protected ByteBuffer getValue(int i)
    {
        return values[i];
    }

    @Override
    public T setBool(int i, boolean v)
    {
        return set(i, v, Boolean.class);
    }

    @Override
    public T setByte(int i, byte v)
    {
        return set(i, v, Byte.class);
    }

    @Override
    public T setShort(int i, short v)
    {
        return set(i, v, Short.class);
    }

    @Override
    public T setInt(int i, int v)
    {
        return set(i, v, Integer.class);
    }

    @Override
    public T setLong(int i, long v)
    {
        return set(i, v, Long.class);
    }

    @Override
    public T setTimestamp(int i, Date v)
    {
        return set(i, v, Date.class);
    }

    @Override
    public T setDate(int i, LocalDate v)
    {
        return setValue(i, codecFor(i, LocalDate.class).serialize(v));
    }

    @Override
    public T setTime(int i, long v)
    {
        return set(i, v, Long.class);
    }

    @Override
    public T setFloat(int i, float v)
    {
        return set(i, v, Float.class);
    }

    @Override
    public T setDouble(int i, double v)
    {
        return set(i, v, Double.class);
    }

    @Override
    public T setString(int i, String v)
    {
        return set(i, v, String.class);
    }

    @Override
    public T setBytes(int i, ByteBuffer v)
    {
        return set(i, v, ByteBuffer.class);
    }

    @Override
    public T setBytesUnsafe(int i, ByteBuffer v)
    {
        return setValue(i, v == null ? null : v.duplicate());
    }

    @Override
    public T setVarint(int i, BigInteger v)
    {
        return set(i, v, BigInteger.class);
    }

    @Override
    public T setDecimal(int i, BigDecimal v)
    {
        return set(i, v, BigDecimal.class);
    }

    @Override
    public T setUUID(int i, UUID v)
    {
        return set(i, v, UUID.class);
    }

    @Override
    public T setInet(int i, InetAddress v)
    {
        return set(i, v, InetAddress.class);
    }

    @Override
    public <E> T setList(int i, List<E> v)
    {
        return setValue(i, codecFor(i).serialize(v));
    }

    @Override
    public <E> T setList(int i, List<E> v, Class<E> elementsClass)
    {
        return setValue(i, codecFor(i, TypeTokens.listOf(elementsClass)).serialize(v));
    }

    @Override
    public <E> T setList(int i, List<E> v, TypeToken<E> elementsType)
    {
        return setValue(i, codecFor(i, TypeTokens.listOf(elementsType)).serialize(v));
    }

    @Override
    public <K, V> T setMap(int i, Map<K, V> v)
    {
        return setValue(i, codecFor(i).serialize(v));
    }

    @Override
    public <K, V> T setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass)
    {
        return setValue(
        i, codecFor(i, TypeTokens.mapOf(keysClass, valuesClass)).serialize(v));
    }

    @Override
    public <K, V> T setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType)
    {
        return setValue(
        i, codecFor(i, TypeTokens.mapOf(keysType, valuesType)).serialize(v));
    }

    @Override
    public <E> T setSet(int i, Set<E> v)
    {
        return setValue(i, codecFor(i).serialize(v));
    }

    @Override
    public <E> T setSet(int i, Set<E> v, Class<E> elementsClass)
    {
        return setValue(i, codecFor(i, TypeTokens.setOf(elementsClass)).serialize(v));
    }

    @Override
    public <E> T setSet(int i, Set<E> v, TypeToken<E> elementsType)
    {
        return setValue(i, codecFor(i, TypeTokens.setOf(elementsType)).serialize(v));
    }

    @Override
    public T setVector(int i, List<T> v)
    {
        return setValue(i, codecFor(i, List.class).serialize(v));
    }

    @Override
    public T setUDTValue(int i, UDTValue v)
    {
        return setValue(i, codecFor(i, UDTValue.class).serialize(v));
    }

    @Override
    public T setTupleValue(int i, TupleValue v)
    {
        return setValue(i, codecFor(i, TupleValue.class).serialize(v));
    }

    @Override
    public <V> T set(int i, V v, Class<V> targetClass)
    {
        return set(i, v, codecFor(i, targetClass));
    }

    @Override
    public <V> T set(int i, V v, TypeToken<V> targetType)
    {
        return set(i, v, codecFor(i, targetType));
    }

    @Override
    public <V> T set(int i, V v, TypeCodec<V, ?> codec)
    {
        checkType(i, codec.getCqlType().getName());
        return setValue(i, codec.serialize(v));
    }

    @Override
    public T setToNull(int i)
    {
        return setValue(i, null);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AbstractAddressableByIndexData)) return false;

        AbstractAddressableByIndexData<?> that = (AbstractAddressableByIndexData<?>) o;
        if (values.length != that.values.length) return false;

        if (this.protocolVersion != that.protocolVersion) return false;

        // Deserializing each value is slightly inefficient, but comparing
        // the bytes could in theory be wrong (for varint for instance, 2 values
        // can have different binary representation but be the same value due to
        // leading zeros). So we don't take any risk.
        for (int i = 0; i < values.length; i++)
        {
            DataType thisType = getType(i);
            DataType thatType = that.getType(i);
            if (!thisType.equals(thatType)) return false;

            Object thisValue = this.codecFor(i).deserialize(this.values[i]);
            Object thatValue = that.codecFor(i).deserialize(that.values[i]);
            if (!Objects.equals(thisValue, thatValue)) return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        // Same as equals
        int hash = 31;
        for (int i = 0; i < values.length; i++)
            hash +=
            values[i] == null ? 1 : codecFor(i).deserialize(values[i]).hashCode();
        return hash;
    }
}
