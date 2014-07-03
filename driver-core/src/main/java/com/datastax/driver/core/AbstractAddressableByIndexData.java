package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.exceptions.InvalidTypeException;

abstract class AbstractAddressableByIndexData<T extends SettableByIndexData<T>> extends AbstractGettableByIndexData implements SettableByIndexData<T> {

    final ByteBuffer[] values;

    protected AbstractAddressableByIndexData(int protocolVersion, int size) {
        super(protocolVersion);
        this.values = new ByteBuffer[size];
    }

    @SuppressWarnings("unchecked")
    protected T setValue(int i, ByteBuffer value) {
        values[i] = value;
        return (T)this;
    }

    protected ByteBuffer getValue(int i) {
        return values[i];
    }

    public T setBool(int i, boolean v) {
        checkType(i, DataType.Name.BOOLEAN);
        return setValue(i, TypeCodec.BooleanCodec.instance.serializeNoBoxing(v));
    }

    public T setInt(int i, int v) {
        checkType(i, DataType.Name.INT);
        return setValue(i, TypeCodec.IntCodec.instance.serializeNoBoxing(v));
    }

    public T setLong(int i, long v) {
        checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);
        return setValue(i, TypeCodec.LongCodec.instance.serializeNoBoxing(v));
    }

    public T setDate(int i, Date v) {
        checkType(i, DataType.Name.TIMESTAMP);
        return setValue(i, v == null ? null : TypeCodec.DateCodec.instance.serialize(v));
    }

    public T setFloat(int i, float v) {
        checkType(i, DataType.Name.FLOAT);
        return setValue(i, TypeCodec.FloatCodec.instance.serializeNoBoxing(v));
    }

    public T setDouble(int i, double v) {
        checkType(i, DataType.Name.DOUBLE);
        return setValue(i, TypeCodec.DoubleCodec.instance.serializeNoBoxing(v));
    }

    public T setString(int i, String v) {
        DataType.Name type = checkType(i, DataType.Name.VARCHAR, DataType.Name.TEXT, DataType.Name.ASCII);
        switch (type) {
            case ASCII:
                return setValue(i, v == null ? null : TypeCodec.StringCodec.asciiInstance.serialize(v));
            case TEXT:
            case VARCHAR:
                return setValue(i, v == null ? null : TypeCodec.StringCodec.utf8Instance.serialize(v));
            default:
                throw new AssertionError();
        }
    }

    public T setBytes(int i, ByteBuffer v) {
        checkType(i, DataType.Name.BLOB);
        return setBytesUnsafe(i, v);
    }

    public T setBytesUnsafe(int i, ByteBuffer v) {
        return setValue(i, v == null ? null : v.duplicate());
    }

    public T setVarint(int i, BigInteger v) {
        checkType(i, DataType.Name.VARINT);
        return setValue(i, v == null ? null : TypeCodec.BigIntegerCodec.instance.serialize(v));
    }

    public T setDecimal(int i, BigDecimal v) {
        checkType(i, DataType.Name.DECIMAL);
        return setValue(i, v == null ? null : TypeCodec.DecimalCodec.instance.serialize(v));
    }

    public T setUUID(int i, UUID v) {
        DataType.Name type = checkType(i, DataType.Name.UUID, DataType.Name.TIMEUUID);

        if (v == null)
            return setValue(i, null);

        if (type == DataType.Name.TIMEUUID && v.version() != 1)
            throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", v));

        return type == DataType.Name.UUID
             ? setValue(i, TypeCodec.UUIDCodec.instance.serialize(v))
             : setValue(i, TypeCodec.TimeUUIDCodec.instance.serialize(v));
    }

    public T setInet(int i, InetAddress v) {
        checkType(i, DataType.Name.INET);
        return setValue(i, v == null ? null : TypeCodec.InetCodec.instance.serialize(v));
    }

    public <E> T setList(int i, List<E> v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a list", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // If the list is empty, it will never fail validation, but otherwise we should check the list given if of the right type
        if (!v.isEmpty()) {
            // Ugly? Yes
            Class<?> providedClass = v.get(0).getClass();
            Class<?> expectedClass = type.getTypeArguments().get(0).asJavaClass();

            if (!expectedClass.isAssignableFrom(providedClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting list of %s but provided list of %s", getName(i), type, expectedClass, providedClass));
        }
        return setValue(i, type.codec(protocolVersion).serialize(v));
    }

    public <K, V> T setMap(int i, Map<K, V> v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.MAP)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a map", getName(i), type));

        if (v == null)
            return setValue(i, null);

        if (!v.isEmpty()) {
            // Ugly? Yes
            Map.Entry<K, V> entry = v.entrySet().iterator().next();
            Class<?> providedKeysClass = entry.getKey().getClass();
            Class<?> providedValuesClass = entry.getValue().getClass();

            Class<?> expectedKeysClass = type.getTypeArguments().get(0).getName().javaType;
            Class<?> expectedValuesClass = type.getTypeArguments().get(1).getName().javaType;
            if (!expectedKeysClass.isAssignableFrom(providedKeysClass) || !expectedValuesClass.isAssignableFrom(providedValuesClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting map of %s->%s but provided map of %s->%s", getName(i), type, expectedKeysClass, expectedValuesClass, providedKeysClass, providedValuesClass));
        }
        return setValue(i, type.codec(protocolVersion).serialize(v));
    }

    public <E> T setSet(int i, Set<E> v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a set", getName(i), type));

        if (v == null)
            return setValue(i, null);

        if (!v.isEmpty()) {
            // Ugly? Yes
            Class<?> providedClass = v.iterator().next().getClass();
            Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;

            if (!expectedClass.isAssignableFrom(providedClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting set of %s but provided set of %s", getName(i), type, expectedClass, providedClass));
        }
        return setValue(i, type.codec(protocolVersion).serialize(v));
    }

    public T setUDTValue(int i, UDTValue v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.UDT)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a UDT", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // UDT always use the V3 protocol version to encode values
        return setValue(i, type.codec(3).serialize(v));
    }

    public T setTupleValue(int i, TupleValue v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.TUPLE)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a tuple", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // Tuples always user the V3 protocol version to encode values
        return setValue(i, type.codec(3).serialize(v));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractAddressableByIndexData))
            return false;

        AbstractAddressableByIndexData<?> that = (AbstractAddressableByIndexData<?>)o;
        if (values.length != that.values.length)
            return false;

        // Deserializing each value is slightly inefficient, but comparing
        // the bytes could in theory be wrong (for varint for instance, 2 values
        // can have different binary representation but be the same value due to
        // leading zeros). So we don't take any risk.
        for (int i = 0; i < values.length; i++) {
            DataType thisType = getType(i);
            DataType thatType = that.getType(i);
            if (!thisType.equals(thatType))
                return false;

            if ((values[i] == null) != (that.values[i] == null))
                return false;

            if (values[i] != null && !(thisType.deserialize(values[i], protocolVersion).equals(thatType.deserialize(that.values[i], protocolVersion))))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // Same as equals
        int hash = 31;
        for (int i = 0; i < values.length; i++)
            hash += values[i] == null ? 1 : getType(i).deserialize(values[i], protocolVersion).hashCode();
        return hash;
    }
}
