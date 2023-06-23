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

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.reflect.TypeToken;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Codec that can serialize and deserialize to and from a given {@link #getCqlType() CQL type} and
 * a given {@link #getJavaType() Java Type}.
 *
 * <p>
 *
 * <h3>Serializing and deserializing</h3>
 *
 * <p>Two methods handle the serialization and deserialization of Java types into CQL types
 * according to the native protocol specifications:
 *
 * <ol>
 * <li>{@link #serialize(Object)}: used to serialize from the codec's Java type
 * to a {@link ByteBuffer} instance corresponding to the codec's CQL type;
 * <li>{@link #deserialize(ByteBuffer)}: used to deserialize a {@link ByteBuffer}
 * instance corresponding to the codec's CQL type to the codec's Java type.
 * </ol>
 *
 * <p>
 *
 * <h3>Inspection</h3>
 *
 * <p>Codecs also have the following inspection methods:
 *
 * <p>
 *
 * <ol>
 * <li>{@link #accepts(DataType)}: returns true if the codec can deserialize the given CQL type;
 * <li>{@link #accepts(TypeToken)}: returns true if the codec can serialize the given Java type;
 * <li>{@link #accepts(Object)}; returns true if the codec can serialize the given object.
 * </ol>
 *
 * <p>
 *
 * <h3>Implementation notes</h3>
 *
 * <p>
 *
 * <ol>
 * <li>TypeCodec implementations <em>must</em> be thread-safe.
 * <li>TypeCodec implementations <em>must</em> perform fast and never block.
 * <li>TypeCodec implementations <em>must</em> support all native protocol versions; it is not
 * possible to use different codecs for the same types but under different protocol versions.
 * <li>TypeCodec implementations must comply with the native protocol specifications; failing to
 * do so will result in unexpected results and could cause the driver to crash.
 * <li>TypeCodec implementations <em>should</em> be stateless and immutable.
 * <li>TypeCodec implementations <em>should</em> interpret {@code null} values and empty
 * ByteBuffers (i.e. <code>{@link ByteBuffer#remaining()} == 0</code>) in a
 * <em>reasonable</em> way; usually, {@code NULL} CQL values should map to {@code null}
 * references, but exceptions exist; e.g. for varchar types, a {@code NULL} CQL value maps to
 * a {@code null} reference, whereas an empty buffer maps to an empty String. For collection
 * types, it is also admitted that {@code NULL} CQL values map to empty Java collections
 * instead of {@code null} references. In any case, the codec's behavior in respect to {@code
 * null} values and empty ByteBuffers should be clearly documented.
 * <li>When deserializing, TypeCodec implementations should not consume {@link ByteBuffer}
 * instances by performing relative read operations that modify their current position; codecs
 * should instead prefer absolute read methods, or, if necessary, they should {@link
 * ByteBuffer#duplicate() duplicate} their byte buffers prior to reading them.
 * </ol>
 *
 * @param <T> The codec's Java type
 */
@SuppressWarnings("UnstableApiUsage") // Suppress warnings about TypeToken
public abstract class TypeCodec<T, I>
{

    /**
     * Return the default codec for the CQL type {@code boolean}. The returned codec maps the CQL type
     * {@code boolean} into the Java type {@link Boolean}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code boolean}.
     */
    public static BooleanCodec cboolean()
    {
        return BooleanCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code tinyint}. The returned codec maps the CQL type
     * {@code tinyint} into the Java type {@link Byte}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code tinyint}.
     */
    public static TinyIntCodec tinyInt()
    {
        return TinyIntCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code smallint}. The returned codec maps the CQL
     * type {@code smallint} into the Java type {@link Short}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code smallint}.
     */
    public static PrimitiveShortCodec smallInt()
    {
        return SmallIntCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code int}. The returned codec maps the CQL type
     * {@code int} into the Java type {@link Integer}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code int}.
     */
    public static PrimitiveIntCodec cint()
    {
        return IntCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code bigint}. The returned codec maps the CQL type
     * {@code bigint} into the Java type {@link Long}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code bigint}.
     */
    public static PrimitiveLongCodec bigint()
    {
        return BigintCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code counter}. The returned codec maps the CQL type
     * {@code counter} into the Java type {@link Long}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code counter}.
     */
    public static PrimitiveLongCodec counter()
    {
        return CounterCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code float}. The returned codec maps the CQL type
     * {@code float} into the Java type {@link Float}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code float}.
     */
    public static PrimitiveFloatCodec cfloat()
    {
        return FloatCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code double}. The returned codec maps the CQL type
     * {@code double} into the Java type {@link Double}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code double}.
     */
    public static DoubleCodec cdouble()
    {
        return DoubleCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code varint}. The returned codec maps the CQL type
     * {@code varint} into the Java type {@link BigInteger}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code varint}.
     */
    public static VarintCodec varint()
    {
        return VarintCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code decimal}. The returned codec maps the CQL type
     * {@code decimal} into the Java type {@link BigDecimal}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code decimal}.
     */
    public static DecimalCodec decimal()
    {
        return DecimalCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code ascii}. The returned codec maps the CQL type
     * {@code ascii} into the Java type {@link String}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code ascii}.
     */
    public static AsciiCodec ascii()
    {
        return AsciiCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code varchar}. The returned codec maps the CQL type
     * {@code varchar} into the Java type {@link String}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code varchar}.
     */
    public static VarcharCodec varchar()
    {
        return VarcharCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code blob}. The returned codec maps the CQL type
     * {@code blob} into the Java type {@link ByteBuffer}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code blob}.
     */
    public static BlobCodec blob()
    {
        return BlobCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code date}. The returned codec maps the CQL type
     * {@code date} into the Java type {@link LocalDate}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code date}.
     */
    public static DateCodec date()
    {
        return DateCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code time}. The returned codec maps the CQL type
     * {@code time} into the Java type {@link Long}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code time}.
     */
    public static TimeCodec time()
    {
        return TimeCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code timestamp}. The returned codec maps the CQL
     * type {@code timestamp} into the Java type {@link Date}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code timestamp}.
     */
    public static TimestampCodec timestamp()
    {
        return TimestampCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code uuid}. The returned codec maps the CQL type
     * {@code uuid} into the Java type {@link UUID}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code uuid}.
     */
    public static UUIDCodec uuid()
    {
        return UUIDCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code timeuuid}. The returned codec maps the CQL
     * type {@code timeuuid} into the Java type {@link UUID}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code timeuuid}.
     */
    public static TimeUUIDCodec timeUUID()
    {
        return TimeUUIDCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code inet}. The returned codec maps the CQL type
     * {@code inet} into the Java type {@link InetAddress}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code inet}.
     */
    public static InetCodec inet()
    {
        return InetCodec.instance;
    }

    /**
     * Return a newly-created codec for the CQL type {@code list} whose element type is determined by
     * the given element codec. The returned codec maps the CQL type {@code list} into the Java type
     * {@link List}. This method does not cache returned instances and returns a newly-allocated
     * object at each invocation.
     *
     * @param elementCodec the codec that will handle elements of this list.
     * @return A newly-created codec for CQL type {@code list}.
     */
    public static <T, I> TypeCodec<List<T>, List<I>> list(TypeCodec<T, I> elementCodec)
    {
        return new ListCodec<>(elementCodec);
    }

    /**
     * Return a newly-created codec for the CQL type {@code set} whose element type is determined by
     * the given element codec. The returned codec maps the CQL type {@code set} into the Java type
     * {@link Set}. This method does not cache returned instances and returns a newly-allocated object
     * at each invocation.
     *
     * @param elementCodec the codec that will handle elements of this set.
     * @return A newly-created codec for CQL type {@code set}.
     */
    public static <T, I> TypeCodec<Set<T>, Set<I>> set(TypeCodec<T, I> elementCodec)
    {
        return new SetCodec<>(elementCodec);
    }

    /**
     * Return a newly-created codec for the CQL type {@code map} whose key type and value type are
     * determined by the given codecs. The returned codec maps the CQL type {@code map} into the Java
     * type {@link Map}. This method does not cache returned instances and returns a newly-allocated
     * object at each invocation.
     *
     * @param keyCodec   the codec that will handle keys of this map.
     * @param valueCodec the codec that will handle values of this map.
     * @return A newly-created codec for CQL type {@code map}.
     */
    public static <K, V, KI, VI> TypeCodec<Map<K, V>, Map<KI, VI>> map(TypeCodec<K, KI> keyCodec, TypeCodec<V, VI> valueCodec)
    {
        return new MapCodec<>(keyCodec, valueCodec);
    }

    /**
     * Return a newly-created codec for the given CQL vector type. The returned codec maps the vector
     * type into the Java type {@link List}. This method does not cache returned instances and
     * returns a newly-allocated object at each invocation.
     *
     * @param type the vector type this codec should handle.
     * @return A newly-created codec for the given CQL tuple type.
     */
    public static <E, I> TypeCodec<List<E>, List<I>> vector(VectorType type, TypeCodec<E, I> valueCodec)
    {
        return new VectorCodec<>(type, valueCodec);
    }

    /**
     * Return a newly-created codec for the given user-defined CQL type. The returned codec maps the
     * user-defined type into the Java type {@link UDTValue}. This method does not cache returned
     * instances and returns a newly-allocated object at each invocation.
     *
     * @param type the user-defined type this codec should handle.
     * @return A newly-created codec for the given user-defined CQL type.
     */
    public static TypeCodec<UDTValue, ByteBuffer> userType(UserType type)
    {
        CodecRegistry registry = type.getCodecRegistry();
        UserType.Field[] fields = type.getFields();

        List<FieldIdentifier> fieldNames = new ArrayList<>(fields.length);
        List<AbstractType<?>> fieldTypes = new ArrayList<>(fields.length);
        for (UserType.Field field : fields)
        {
            fieldNames.add(FieldIdentifier.forQuoted(field.getName()));
            fieldTypes.add(registry.codecFor(field.getType()).getSerializer());
        }

        return new UDTCodec(type, new org.apache.cassandra.db.marshal.UserType(type.getKeyspace(),
                                                                               ByteBufferUtil.bytes(type.getTypeName()),
                                                                               fieldNames,
                                                                               fieldTypes,
                                                                               false));
    }

    /**
     * Return a newly-created codec for the given CQL tuple type. The returned codec maps the tuple
     * type into the Java type {@link TupleValue}. This method does not cache returned instances and
     * returns a newly-allocated object at each invocation.
     *
     * @param type the tuple type this codec should handle.
     * @return A newly-created codec for the given CQL tuple type.
     */
    public static TypeCodec<TupleValue, ByteBuffer> tuple(TupleType type)
    {
        CodecRegistry registry = type.getCodecRegistry();
        List<DataType> types = type.getComponentTypes();
        List<AbstractType<?>> serializers = new ArrayList<>(types.size());
        for (DataType dataType : types)
            serializers.add(registry.codecFor(dataType).getSerializer());
        return new TupleCodec(type, new org.apache.cassandra.db.marshal.TupleType(serializers));
    }

    /**
     * Return a newly-created codec for the given CQL custom type.
     *
     * <p>The returned codec maps the custom type into the Java type {@link ByteBuffer}, thus
     * providing a (very lightweight) support for Cassandra types that do not have a CQL equivalent.
     *
     * <p>This method does not cache returned instances and returns a newly-allocated object at each
     * invocation.
     *
     * @param type the custom type this codec should handle.
     * @return A newly-created codec for the given CQL custom type.
     */
    public static TypeCodec<ByteBuffer, ByteBuffer> custom(DataType.CustomType type)
    {
        return new CustomCodec(type);
    }

    /**
     * Returns the default codec for the {@link DataType#duration() Duration type}.
     *
     * <p>This codec maps duration types to the driver's built-in {@link Duration} class, thus
     * providing a more user-friendly mapping than the low-level mapping provided by regular {@link
     * #custom(DataType.CustomType) custom type codecs}.
     *
     * <p>The returned instance is a singleton.
     *
     * @return the default codec for the Duration type.
     */
    public static TypeCodec<Duration, org.apache.cassandra.cql3.Duration> duration()
    {
        return DurationCodec.instance;
    }

    private final TypeToken<T> javaType;
    final AbstractType<I> serializer;
    final DataType cqlType;

    /**
     * This constructor can only be used for non parameterized types. For parameterized ones, please
     * use {@link #TypeCodec(DataType, AbstractType, TypeToken)} instead.
     *
     * @param javaClass The Java class this codec serializes from and deserializes to.
     */
    protected TypeCodec(DataType cqlType, AbstractType<I> serializer, Class<T> javaClass)
    {
        this(cqlType, serializer, TypeToken.of(javaClass));
    }

    protected TypeCodec(DataType cqlType, AbstractType<I> serializer, TypeToken<T> javaType)
    {
        checkNotNull(cqlType, "cqlType cannot be null");
        checkNotNull(javaType, "javaType cannot be null");
        checkArgument(
        !javaType.isPrimitive(),
        "Cannot create a codec for a primitive Java type (%s), please use the wrapper type instead",
        javaType);
        this.cqlType = cqlType;
        this.serializer = serializer;
        this.javaType = javaType;
    }

    /**
     * Return the Java type that this codec deserializes to and serializes from.
     *
     * @return The Java type this codec deserializes to and serializes from.
     */
    public TypeToken<T> getJavaType()
    {
        return javaType;
    }

    /**
     * Return the CQL type that this codec deserializes from and serializes to.
     *
     * @return The Java type this codec deserializes from and serializes to.
     */
    public DataType getCqlType()
    {
        return cqlType;
    }

    public AbstractType<I> getSerializer()
    {
        return serializer;
    }

    @SuppressWarnings("unchecked")
    public T toDriver(I value)
    {
        return (T) value;
    }

    @SuppressWarnings("unchecked")
    public I fromDriver(T value)
    {
        return (I) value;
    }

    /**
     * Serialize the given value according to the CQL type handled by this codec.
     *
     * <p>Implementation notes:
     *
     * <ol>
     * <li>Null values should be gracefully handled and no exception should be raised; these should
     * be considered as the equivalent of a NULL CQL value;
     * <li>Codecs for CQL collection types should not permit null elements;
     * <li>Codecs for CQL collection types should treat a {@code null} input as the equivalent of an
     * empty collection.
     * </ol>
     *
     * @param value An instance of T; may be {@code null}.
     * @return A {@link ByteBuffer} instance containing the serialized form of T
     * @throws InvalidTypeException if the given value does not have the expected type
     */
    public ByteBuffer serialize(T value)
    {
        return serializer.decompose(fromDriver(value));
    }

    /**
     * Deserialize the given {@link ByteBuffer} instance according to the CQL type handled by this
     * codec.
     *
     * <p>Implementation notes:
     *
     * <ol>
     * <li>Null or empty buffers should be gracefully handled and no exception should be raised;
     * these should be considered as the equivalent of a NULL CQL value and, in most cases,
     * should map to {@code null} or a default value for the corresponding Java type, if
     * applicable;
     * <li>Codecs for CQL collection types should clearly document whether they return immutable
     * collections or not (note that the driver's default collection codecs return
     * <em>mutable</em> collections);
     * <li>Codecs for CQL collection types should avoid returning {@code null}; they should return
     * empty collections instead (the driver's default collection codecs all comply with this
     * rule).
     * <li>The provided {@link ByteBuffer} should never be consumed by read operations that modify
     * its current position; if necessary, {@link ByteBuffer#duplicate()} duplicate} it before
     * consuming.
     * </ol>
     *
     * @param bytes A {@link ByteBuffer} instance containing the serialized form of T; may be {@code
     *              null} or empty.
     * @return An instance of T
     * @throws InvalidTypeException if the given {@link ByteBuffer} instance cannot be deserialized
     */
    public final T deserialize(ByteBuffer bytes) throws InvalidTypeException
    {
        return bytes == null ? null : toDriver(serializer.compose(bytes));
    }

    /**
     * Return {@code true} if this codec is capable of serializing the given {@code javaType}.
     *
     * <p>The implementation is <em>invariant</em> with respect to the passed argument (through the
     * usage of {@link TypeToken#equals(Object)} and <em>it's strongly recommended not to modify this
     * behavior</em>. This means that a codec will only ever return {@code true} for the
     * <em>exact</em> Java type that it has been created for.
     *
     * <p>If the argument represents a Java primitive type, its wrapper type is considered instead.
     *
     * @param javaType The Java type this codec should serialize from and deserialize to; cannot be
     *                 {@code null}.
     * @return {@code true} if the codec is capable of serializing the given {@code javaType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code javaType} is {@code null}.
     */
    public boolean accepts(TypeToken<?> javaType)
    {
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return this.javaType.equals(javaType.wrap());
    }

    /**
     * Return {@code true} if this codec is capable of serializing the given {@code javaType}.
     *
     * <p>This implementation simply calls {@link #accepts(TypeToken)}.
     *
     * @param javaType The Java type this codec should serialize from and deserialize to; cannot be
     *                 {@code null}.
     * @return {@code true} if the codec is capable of serializing the given {@code javaType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code javaType} is {@code null}.
     */
    public boolean accepts(Class<?> javaType)
    {
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return accepts(TypeToken.of(javaType));
    }

    /**
     * Return {@code true} if this codec is capable of deserializing the given {@code cqlType}.
     *
     * @param cqlType The CQL type this codec should deserialize from and serialize to; cannot be
     *                {@code null}.
     * @return {@code true} if the codec is capable of deserializing the given {@code cqlType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code cqlType} is {@code null}.
     */
    public boolean accepts(DataType cqlType)
    {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        return this.cqlType.equals(cqlType);
    }

    /**
     * Return {@code true} if this codec is capable of serializing the given object. Note that the
     * object's Java type is inferred from the object' runtime (raw) type, contrary to {@link
     * #accepts(TypeToken)} which is capable of handling generic types.
     *
     * <p>This method is intended mostly to be used by the QueryBuilder when no type information is
     * available when the codec is used.
     *
     * <p>Implementation notes:
     *
     * <ol>
     * <li>The default implementation is <em>covariant</em> with respect to the passed argument
     * (through the usage of {@code TypeToken#isAssignableFrom(TypeToken)} or {@link
     * TypeToken#isSupertypeOf(Type)}) and <em>it's strongly recommended not to modify this
     * behavior</em>. This means that, by default, a codec will accept <em>any subtype</em> of
     * the Java type that it has been created for.
     * <li>The base implementation provided here can only handle non-parameterized types; codecs
     * handling parameterized types, such as collection types, must override this method and
     * perform some sort of "manual" inspection of the actual type parameters.
     * <li>Similarly, codecs that only accept a partial subset of all possible values must override
     * this method and manually inspect the object to check if it complies or not with the
     * codec's limitations.
     * </ol>
     *
     * @param value The Java type this codec should serialize from and deserialize to; cannot be
     *              {@code null}.
     * @return {@code true} if the codec is capable of serializing the given {@code javaType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code value} is {@code null}.
     */
    public boolean accepts(Object value)
    {
        checkNotNull(value, "Parameter value cannot be null");
        return this.javaType.isSupertypeOf(TypeToken.of(value.getClass()));
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s <-> %s]", this.getClass().getSimpleName(), cqlType, javaType);
    }

    public abstract static class SimpleCodec<T> extends TypeCodec<T, T>
    {
        public SimpleCodec(DataType cqlType, AbstractType<T> serializer, Class<T> javaClass)
        {
            super(cqlType, serializer, javaClass);
        }
    }

    /**
     * A codec that is capable of handling primitive shorts, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveShortCodec extends SimpleCodec<Short>
    {
        PrimitiveShortCodec(DataType cqlType)
        {
            super(cqlType, ShortType.instance, Short.class);
        }
    }

    /**
     * A codec that is capable of handling primitive ints, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveIntCodec extends SimpleCodec<Integer>
    {
        PrimitiveIntCodec(DataType cqlType)
        {
            super(cqlType, Int32Type.instance, Integer.class);
        }
    }

    /**
     * A codec that is capable of handling primitive longs, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveLongCodec extends SimpleCodec<Long>
    {
        PrimitiveLongCodec(DataType cqlType)
        {
            super(cqlType, LongType.instance, Long.class);
        }
    }

    /**
     * A codec that is capable of handling primitive floats, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveFloatCodec extends SimpleCodec<Float>
    {
        PrimitiveFloatCodec(DataType cqlType)
        {
            super(cqlType, FloatType.instance, Float.class);
        }
    }

    /**
     * Base class for codecs handling CQL string types such as {@link DataType#varchar()}, {@link
     * DataType#text()} or {@link DataType#ascii()}.
     */
    public abstract static class StringCodec extends SimpleCodec<String>
    {
        private StringCodec(DataType cqlType, AbstractType<String> serializer)
        {
            super(cqlType, serializer, String.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#varchar()} to a Java {@link String}. Note that this codec
     * also handles {@link DataType#text()}, which is merely an alias for {@link DataType#varchar()}.
     */
    public static class VarcharCodec extends StringCodec
    {
        private static final VarcharCodec instance = new VarcharCodec();

        private VarcharCodec()
        {
            super(DataType.varchar(), UTF8Type.instance);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#ascii()} to a Java {@link String}.
     */
    public static class AsciiCodec extends StringCodec
    {
        private static final AsciiCodec instance = new AsciiCodec();

        private AsciiCodec()
        {
            super(DataType.ascii(), AsciiType.instance);
        }
    }

    /**
     * Base class for codecs handling CQL 8-byte integer types such as {@link DataType#bigint()},
     * {@link DataType#counter()} or {@link DataType#time()}.
     */
    public abstract static class LongCodec extends PrimitiveLongCodec
    {
        private LongCodec(DataType cqlType)
        {
            super(cqlType);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#bigint()} to a Java {@link Long}.
     */
    public static class BigintCodec extends LongCodec
    {
        private static final BigintCodec instance = new BigintCodec();

        private BigintCodec()
        {
            super(DataType.bigint());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#counter()} to a Java {@link Long}.
     */
    public static class CounterCodec extends LongCodec
    {
        private static final CounterCodec instance = new CounterCodec();

        private CounterCodec()
        {
            super(DataType.counter());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#blob()} to a Java {@link ByteBuffer}.
     */
    public static class BlobCodec extends SimpleCodec<ByteBuffer>
    {

        private static final BlobCodec instance = new BlobCodec();

        private BlobCodec()
        {
            super(DataType.blob(), BytesType.instance, ByteBuffer.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#custom(String) custom} type to a Java {@link ByteBuffer}.
     * Note that no instance of this codec is part of the default set of codecs used by the Java
     * driver; instances of this codec must be manually registered.
     */
    public static class CustomCodec extends SimpleCodec<ByteBuffer>
    {
        private CustomCodec(DataType custom)
        {
            super(custom, BytesType.instance, ByteBuffer.class);
            assert custom.getName() == DataType.Name.CUSTOM;
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cboolean()} to a Java {@link Boolean}.
     */
    public static class BooleanCodec extends SimpleCodec<Boolean>
    {
        private static final BooleanCodec instance = new BooleanCodec();

        private BooleanCodec()
        {
            super(DataType.cboolean(), BooleanType.instance, Boolean.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#decimal()} to a Java {@link BigDecimal}.
     */
    public static class DecimalCodec extends SimpleCodec<BigDecimal>
    {
        private static final DecimalCodec instance = new DecimalCodec();

        private DecimalCodec()
        {
            super(DataType.decimal(), DecimalType.instance, BigDecimal.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cdouble()} to a Java {@link Double}.
     */
    public static class DoubleCodec extends SimpleCodec<Double>
    {
        private static final DoubleCodec instance = new DoubleCodec();

        private DoubleCodec()
        {
            super(DataType.cdouble(), DoubleType.instance, Double.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cfloat()} to a Java {@link Float}.
     */
    public static class FloatCodec extends PrimitiveFloatCodec
    {

        private static final FloatCodec instance = new FloatCodec();

        private FloatCodec()
        {
            super(DataType.cfloat());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#inet()} to a Java {@link InetAddress}.
     */
    public static class InetCodec extends SimpleCodec<InetAddress>
    {

        private static final InetCodec instance = new InetCodec();

        private InetCodec()
        {
            super(DataType.inet(), InetAddressType.instance, InetAddress.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#tinyint()} to a Java {@link Byte}.
     */
    public static class TinyIntCodec extends SimpleCodec<Byte>
    {

        private static final TinyIntCodec instance = new TinyIntCodec();

        private TinyIntCodec()
        {
            super(DataType.tinyint(), ByteType.instance, Byte.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#smallint()} to a Java {@link Short}.
     */
    public static class SmallIntCodec extends PrimitiveShortCodec
    {

        private static final SmallIntCodec instance = new SmallIntCodec();

        private SmallIntCodec()
        {
            super(DataType.smallint());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cint()} to a Java {@link Integer}.
     */
    public static class IntCodec extends PrimitiveIntCodec
    {

        private static final IntCodec instance = new IntCodec();

        private IntCodec()
        {
            super(DataType.cint());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#timestamp()} to a Java {@link Date}.
     */
    public static class TimestampCodec extends SimpleCodec<Date>
    {

        private static final TimestampCodec instance = new TimestampCodec();

        private TimestampCodec()
        {
            super(DataType.timestamp(), TimestampType.instance, Date.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#date()} to the custom {@link LocalDate} class.
     */
    public static class DateCodec extends TypeCodec<LocalDate, Integer>
    {
        private static final DateCodec instance = new DateCodec();

        private DateCodec()
        {
            super(DataType.date(), SimpleDateType.instance, LocalDate.class);
        }

        @Override
        public LocalDate toDriver(Integer value)
        {
            return LocalDate.fromDaysSinceEpoch(CodecUtils.fromCqlDateToDaysSinceEpoch(value));
        }

        @Override
        public Integer fromDriver(LocalDate value)
        {
            return CodecUtils.fromSignedToUnsignedInt(value.getDaysSinceEpoch());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#time()} to a Java {@link Long}.
     */
    public static class TimeCodec extends LongCodec
    {
        private static final TimeCodec instance = new TimeCodec();

        private TimeCodec()
        {
            super(DataType.time());
        }
    }

    /**
     * Base class for codecs handling CQL UUID types such as {@link DataType#uuid()} and {@link
     * DataType#timeuuid()}.
     */
    public abstract static class AbstractUUIDCodec<I> extends TypeCodec<UUID, I>
    {
        private AbstractUUIDCodec(DataType cqlType, AbstractType<I> serializer)
        {
            super(cqlType, serializer, UUID.class);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#uuid()} to a Java {@link UUID}.
     */
    public static class UUIDCodec extends AbstractUUIDCodec<UUID>
    {
        private static final UUIDCodec instance = new UUIDCodec();

        private UUIDCodec()
        {
            super(DataType.uuid(), UUIDType.instance);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#timeuuid()} to a Java {@link UUID}.
     */
    public static class TimeUUIDCodec extends AbstractUUIDCodec<TimeUUID>
    {
        private static final TimeUUIDCodec instance = new TimeUUIDCodec();

        private TimeUUIDCodec()
        {
            super(DataType.timeuuid(), TimeUUIDType.instance);
        }

        @Override
        public UUID toDriver(TimeUUID value)
        {
            return value.asUUID();
        }

        @Override
        public TimeUUID fromDriver(UUID value)
        {
            return TimeUUID.fromUuid(value);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#varint()} to a Java {@link BigInteger}.
     */
    public static class VarintCodec extends SimpleCodec<BigInteger>
    {
        private static final VarintCodec instance = new VarintCodec();

        private VarintCodec()
        {
            super(DataType.varint(), IntegerType.instance, BigInteger.class);
        }
    }

    /**
     * Base class for codecs mapping CQL {@link DataType#list(DataType) lists} and {@link
     * DataType#set(DataType) sets} to Java collections.
     */
    public abstract static class AbstractCollectionCodec<E, I, C extends Collection<E>, CI extends Collection<I>>
    extends TypeCodec<C, CI>
    {

        final TypeCodec<E, I> eltCodec;

        AbstractCollectionCodec(DataType.CollectionType cqlType,
                                CollectionType<CI> serializer,
                                TypeToken<C> javaType,
                                TypeCodec<E, I> eltCodec)
        {
            super(cqlType, serializer, javaType);
            checkArgument(
            cqlType.getName() == DataType.Name.LIST || cqlType.getName() == DataType.Name.SET,
            "Expecting list or set type, got %s",
            cqlType);
            this.eltCodec = eltCodec;
        }

        @Override
        public boolean accepts(Object value)
        {
            if (getJavaType().getRawType().isAssignableFrom(value.getClass()))
            {
                // runtime type ok, now check element type
                Collection<?> coll = (Collection<?>) value;
                if (coll.isEmpty()) return true;
                Object elt = coll.iterator().next();
                return eltCodec.accepts(elt);
            }
            return false;
        }
    }

    /**
     * This codec maps a CQL {@link DataType#list(DataType) list type} to a Java {@link List}.
     * Implementation note: this codec returns mutable, non thread-safe {@link ArrayList} instances.
     */
    public static class ListCodec<T, I> extends AbstractCollectionCodec<T, I, List<T>, List<I>>
    {
        private ListCodec(TypeCodec<T, I> eltCodec)
        {
            super(DataType.list(eltCodec.getCqlType()),
                  ListType.getInstance(eltCodec.getSerializer(), false),
                  TypeTokens.listOf(eltCodec.getJavaType()),
                  eltCodec);
        }

        @Override
        public List<T> toDriver(List<I> value)
        {
            return value.stream().map(eltCodec::toDriver).collect(Collectors.toList());
        }

        @Override
        public List<I> fromDriver(List<T> value)
        {
            return value.stream().map(eltCodec::fromDriver).collect(Collectors.toList());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#set(DataType) set type} to a Java {@link Set}.
     * Implementation note: this codec returns mutable, non thread-safe {@link LinkedHashSet}
     * instances.
     */
    public static class SetCodec<T, I> extends AbstractCollectionCodec<T, I, Set<T>, Set<I>>
    {

        private SetCodec(TypeCodec<T, I> eltCodec)
        {
            super(DataType.set(eltCodec.cqlType),
                  SetType.getInstance(eltCodec.getSerializer(), false),
                  TypeTokens.setOf(eltCodec.getJavaType()),
                  eltCodec);
        }

        @Override
        public Set<T> toDriver(Set<I> value)
        {
            Set<T> result = newInstance(value.size());
            for (I elt : value)
                result.add(eltCodec.toDriver(elt));
            return result;
        }

        @Override
        public Set<I> fromDriver(Set<T> value)
        {
            Set<I> result = newInstance(value.size());
            for (T elt : value)
                result.add(eltCodec.fromDriver(elt));
            return result;
        }

        private static <E> Set<E> newInstance(int size)
        {
            // we need to preserve the order of elements when converting between internal and external representations
            return new LinkedHashSet<>(size);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#map(DataType, DataType) map type} to a Java {@link Map}.
     * Implementation note: this codec returns mutable, non thread-safe {@link LinkedHashMap}
     * instances.
     */
    public static class MapCodec<K, V, KI, VI> extends TypeCodec<Map<K, V>, Map<KI, VI>>
    {
        final TypeCodec<K, KI> keyCodec;
        final TypeCodec<V, VI> valueCodec;

        MapCodec(TypeCodec<K, KI> keyCodec, TypeCodec<V, VI> valueCodec)
        {
            super(
            DataType.map(keyCodec.getCqlType(), valueCodec.getCqlType()),
            MapType.getInstance(keyCodec.getSerializer(), valueCodec.getSerializer(), false),
            TypeTokens.mapOf(keyCodec.getJavaType(), valueCodec.getJavaType()));
            this.keyCodec = keyCodec;
            this.valueCodec = valueCodec;
        }

        @Override
        public boolean accepts(Object value)
        {
            if (value instanceof Map)
            {
                // runtime type ok, now check key and value types
                Map<?, ?> map = (Map<?, ?>) value;
                if (map.isEmpty()) return true;
                Map.Entry<?, ?> entry = map.entrySet().iterator().next();
                return keyCodec.accepts(entry.getKey()) && valueCodec.accepts(entry.getValue());
            }
            return false;
        }

        @Override
        public Map<K, V> toDriver(Map<KI, VI> value)
        {
            Map<K, V> result = newInstance(value.size());
            value.forEach((k, v) -> result.put(keyCodec.toDriver(k), valueCodec.toDriver(v)));
            return result;
        }

        @Override
        public Map<KI, VI> fromDriver(Map<K, V> value)
        {
            Map<KI, VI> result = newInstance(value.size());
            value.forEach((k, v) -> result.put(keyCodec.fromDriver(k), valueCodec.fromDriver(v)));
            return result;
        }

        private static <K, V> Map<K, V> newInstance(int size)
        {
            // we need to preserve the order of elements when converting between internal and external representations
            return new LinkedHashMap<>(size);
        }
    }

    /**
     * This codec maps a CQL {@link UserType} to a {@link UDTValue}.
     */
    public static class UDTCodec extends TypeCodec<UDTValue, ByteBuffer>
    {
        private final UserType definition;
        private final org.apache.cassandra.db.marshal.UserType serializer;

        UDTCodec(UserType definition, org.apache.cassandra.db.marshal.UserType serializer)
        {
            super(definition, serializer, TypeToken.of(UDTValue.class));
            this.definition = definition;
            this.serializer = serializer;
        }

        @Override
        public UDTValue toDriver(ByteBuffer value)
        {
            UDTValue tuple = newInstance();
            int i = 0;
            for (ByteBuffer v : serializer.split(ByteBufferAccessor.instance, value))
                tuple.setBytesUnsafe(i++, v);
            return tuple;
        }

        @Override
        public ByteBuffer fromDriver(UDTValue value)
        {
            return org.apache.cassandra.db.marshal.UserType.buildValue(value.values);
        }

        /**
         * Return a new instance of {@code T}.
         *
         * @return A new instance of {@code T}.
         */
        protected UDTValue newInstance()
        {
            return definition.newValue();
        }
    }

    /**
     * This codec maps a CQL {@link TupleType tuple} to a {@link TupleValue}.
     */
    public static class TupleCodec extends TypeCodec<TupleValue, ByteBuffer>
    {
        private final TupleType definition;
        private final org.apache.cassandra.db.marshal.TupleType serializer;

        TupleCodec(TupleType definition, org.apache.cassandra.db.marshal.TupleType serializer)
        {
            super(definition, serializer, TypeToken.of(TupleValue.class));
            this.definition = definition;
            this.serializer = serializer;
        }

        @Override
        public TupleValue toDriver(ByteBuffer value)
        {
            TupleValue tuple = newInstance();
            int i = 0;
            for (ByteBuffer v : serializer.split(ByteBufferAccessor.instance, value))
                tuple.setBytesUnsafe(i++, v);
            return tuple;
        }

        @Override
        public ByteBuffer fromDriver(TupleValue value)
        {
            return org.apache.cassandra.db.marshal.TupleType.buildValue(value.values);
        }

        @Override
        public boolean accepts(Object value)
        {
            // a tuple codec should accept tuple values of a different type,
            // provided that the latter is contained in this codec's type.
            return super.accepts(value) && definition.contains(((TupleValue) value).getType());
        }

        /**
         * Return a new instance of {@code T}.
         *
         * @return A new instance of {@code T}.
         */
        protected TupleValue newInstance()
        {
            return definition.newValue();
        }
    }

    public static class DurationCodec extends TypeCodec<Duration, org.apache.cassandra.cql3.Duration>
    {
        private static final DurationCodec instance = new DurationCodec();

        private DurationCodec()
        {
            super(DataType.duration(), DurationType.instance, Duration.class);
        }

        @Override
        public Duration toDriver(org.apache.cassandra.cql3.Duration duration)
        {
            return Duration.newInstance(duration.getMonths(), duration.getDays(), duration.getNanoseconds());
        }

        @Override
        public org.apache.cassandra.cql3.Duration fromDriver(Duration duration)
        {
            return org.apache.cassandra.cql3.Duration.newInstance(duration.getMonths(), duration.getDays(), duration.getNanoseconds());
        }
    }
}
