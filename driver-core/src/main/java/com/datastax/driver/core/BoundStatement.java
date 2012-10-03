package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.transport.Codec;

import org.apache.cassandra.db.marshal.*;

/**
 * A prepared statement with values bound to the bind variables.
 * <p>
 * Once a BoundStatement has values for all the variables of the {@link PreparedStatement}
 * it has been created from, it can executed through {@link Session#executePrepared}.
 */
public class BoundStatement {

    final PreparedStatement statement;
    final ByteBuffer[] values;
    private int remaining;

    BoundStatement(PreparedStatement statement) {
        this.statement = statement;
        this.values = new ByteBuffer[statement.variables().count()];
        this.remaining = values.length;
    }

    /**
     * Returns the prepared statement on which this BoundStatement is based.
     *
     * @return the prepared statement on which this BoundStatement is based.
     */
    public PreparedStatement preparedStatement() {
        return statement;
    }

    /**
     * Returns whether all variables have been bound to values in thi
     * BoundStatement.
     *
     * @return whether all variables are bound.
     */
    public boolean ready() {
        return remaining == 0;
    }

    /**
     * Returns whether the {@code i}th variable has been bound to a value.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().count()}.
     * @return whether the {@code i}th variable has been bound to a value.
     */
    public boolean isSet(int i) {
        metadata().checkBounds(i);
        return values[i] != null;
    }

    /**
     * Returns whether the variable {@code name} has been bound to a value.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, i.e. if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @return whether the {@code i}th variable has been bound to a value.
     */
    public boolean isSet(String name) {
        return isSet(metadata().getIdx(name));
    }

    public BoundStatement bind(Object... values) {
        // TODO
        return null;
    }

    public BoundStatement setBool(int i, boolean v) {
        metadata().checkType(i, DataType.Native.BOOLEAN);
        return setValue(i, BooleanType.instance.decompose(v));
    }

    public BoundStatement setBool(String name, boolean v) {
        return setBool(metadata().getIdx(name), v);
    }

    public BoundStatement setInt(int i, int v) {
        DataType.Native type = metadata().checkType(i, DataType.Native.INT,
                                                       DataType.Native.TIMESTAMP,
                                                       DataType.Native.BIGINT,
                                                       DataType.Native.COUNTER,
                                                       DataType.Native.VARINT);

        switch (type) {
            case INT:
                return setValue(i, Int32Type.instance.decompose(v));
            case TIMESTAMP:
            case BIGINT:
            case COUNTER:
                return setValue(i, LongType.instance.decompose((long)v));
            case VARINT:
                return setValue(i, IntegerType.instance.decompose(BigInteger.valueOf((long)v)));
            default:
                throw new AssertionError();
        }
    }

    public BoundStatement setInt(String name, int v) {
        return setInt(metadata().getIdx(name), v);
    }

    public BoundStatement setLong(int i, long v) {
        DataType.Native type = metadata().checkType(i, DataType.Native.BIGINT,
                                                       DataType.Native.TIMESTAMP,
                                                       DataType.Native.COUNTER,
                                                       DataType.Native.VARINT);

        switch (type) {
            case TIMESTAMP:
            case BIGINT:
            case COUNTER:
                return setValue(i, LongType.instance.decompose(v));
            case VARINT:
                return setValue(i, IntegerType.instance.decompose(BigInteger.valueOf(v)));
            default:
                throw new AssertionError();
        }
    }

    public BoundStatement setLong(String name, long v) {
        return setLong(metadata().getIdx(name), v);
    }

    public BoundStatement setDate(int i, Date v) {
        metadata().checkType(i, DataType.Native.TIMESTAMP);
        return setValue(i, DateType.instance.decompose(v));
    }

    public BoundStatement setDate(String name, Date v) {
        return setDate(metadata().getIdx(name), v);
    }

    public BoundStatement setFloat(int i, float v) {
        DataType.Native type = metadata().checkType(i, DataType.Native.FLOAT,
                                                       DataType.Native.DOUBLE,
                                                       DataType.Native.DECIMAL);

        switch (type) {
            case FLOAT:
                return setValue(i, FloatType.instance.decompose(v));
            case DOUBLE:
                return setValue(i, DoubleType.instance.decompose((double)v));
            case DECIMAL:
                return setValue(i, DecimalType.instance.decompose(BigDecimal.valueOf((double)v)));
            default:
                throw new AssertionError();
        }
    }

    public BoundStatement setFloat(String name, float v) {
        return setFloat(metadata().getIdx(name), v);
    }

    public BoundStatement setDouble(int i, double v) {
        DataType.Native type = metadata().checkType(i, DataType.Native.DOUBLE,
                                                       DataType.Native.DECIMAL);
        switch (type) {
            case DOUBLE:
                return setValue(i, DoubleType.instance.decompose(v));
            case DECIMAL:
                return setValue(i, DecimalType.instance.decompose(BigDecimal.valueOf(v)));
            default:
                throw new AssertionError();
        }
    }

    public BoundStatement setDouble(String name, double v) {
        return setDouble(metadata().getIdx(name), v);
    }

    public BoundStatement setString(int i, String v) {
        DataType.Native type = metadata().checkType(i, DataType.Native.VARCHAR,
                                                       DataType.Native.TEXT,
                                                       DataType.Native.ASCII);
        switch (type) {
            case ASCII:
                return setValue(i, AsciiType.instance.decompose(v));
            case TEXT:
            case VARCHAR:
                return setValue(i, UTF8Type.instance.decompose(v));
            default:
                throw new AssertionError();
        }
    }

    public BoundStatement setString(String name, String v) {
        return setString(metadata().getIdx(name), v);
    }

    public BoundStatement setByteBuffer(int i, ByteBuffer v) {
        return setValue(i, v.duplicate());
    }

    public BoundStatement setByteBuffer(String name, ByteBuffer v) {
        return setByteBuffer(metadata().getIdx(name), v);
    }

    public BoundStatement setBytes(int i, byte[] v) {
        return setValue(i, ByteBuffer.wrap(v));
    }

    public BoundStatement setBytes(String name, byte[] v) {
        return setBytes(metadata().getIdx(name), v);
    }

    public BoundStatement setVarInt(int i, BigInteger v) {
        metadata().checkType(i, DataType.Native.VARINT);
        return setValue(i, IntegerType.instance.decompose(v));
    }

    public BoundStatement setVarInt(String name, BigInteger v) {
        return setVarInt(metadata().getIdx(name), v);
    }

    public BoundStatement setDecimal(int i, BigDecimal v) {
        metadata().checkType(i, DataType.Native.DECIMAL);
        return setValue(i, DecimalType.instance.decompose(v));
    }

    public BoundStatement setDecimal(String name, BigDecimal v) {
        return setDecimal(metadata().getIdx(name), v);
    }

    public BoundStatement setUUID(int i, UUID v) {
        DataType.Native type = metadata().checkType(i, DataType.Native.UUID,
                                                       DataType.Native.TIMEUUID);

        return type == DataType.Native.UUID
             ? setValue(i, UUIDType.instance.decompose(v))
             : setValue(i, TimeUUIDType.instance.decompose(v));
    }

    public BoundStatement setUUID(String name, UUID v) {
        return setUUID(metadata().getIdx(name), v);
    }

    public <T> BoundStatement setList(int i, List<T> v) {
        DataType type = metadata().type(i);
        if (type.kind() != DataType.Kind.COLLECTION || type.asCollection().collectionType() != DataType.Collection.Type.LIST)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a list", metadata().name(i), type));

        // If the list is empty, it will never fail validation, but otherwise we should check the list given if of the right type
        if (!v.isEmpty()) {
            // Ugly? Yes
            Class klass = v.get(0).getClass();

            DataType.Native eltType = (DataType.Native)((DataType.Collection.List)type).getElementsType();
            if (!Codec.isCompatible(eltType, klass))
                throw new InvalidTypeException(String.format("Column %s is a %s, cannot set to a list of %s", metadata().name(i), type, klass));
        }

        return setValue(i, Codec.<List<T>>getCodec(type).decompose(v));
    }

    public <T> BoundStatement setList(String name, List<T> v) {
        return setList(metadata().getIdx(name), v);
    }

    public <K, V> BoundStatement setMap(int i, Map<K, V> v) {
        DataType type = metadata().type(i);
        if (type.kind() != DataType.Kind.COLLECTION || type.asCollection().collectionType() != DataType.Collection.Type.MAP)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a map", metadata().name(i), type));

        if (!v.isEmpty()) {
            // Ugly? Yes
            Map.Entry<K, V> entry = v.entrySet().iterator().next();
            Class keysClass = entry.getKey().getClass();
            Class valuesClass = entry.getValue().getClass();

            DataType.Collection.Map mapType = (DataType.Collection.Map)type;
            DataType.Native keysType = (DataType.Native)mapType.getKeysType();
            DataType.Native valuesType = (DataType.Native)mapType.getValuesType();
            if (!Codec.isCompatible(keysType, keysClass) || !Codec.isCompatible(valuesType, valuesClass))
                throw new InvalidTypeException(String.format("Column %s is a %s, cannot set to a map of %s -> %s", metadata().name(i), type, keysType, valuesType));
        }

        return setValue(i, Codec.<Map<K, V>>getCodec(type).decompose(v));
    }

    public <K, V> BoundStatement setMap(String name, Map<K, V> v) {
        return setMap(metadata().getIdx(name), v);
    }

    public <T> BoundStatement setSet(int i, Set<T> v) {
        DataType type = metadata().type(i);
        if (type.kind() != DataType.Kind.COLLECTION || type.asCollection().collectionType() != DataType.Collection.Type.SET)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a set", metadata().name(i), type));

        if (!v.isEmpty()) {
            // Ugly? Yes
            Class klass = v.iterator().next().getClass();

            DataType.Native eltType = (DataType.Native)((DataType.Collection.Set)type).getElementsType();
            if (!Codec.isCompatible(eltType, klass))
                throw new InvalidTypeException(String.format("Column %s is a %s, cannot set to a set of %s", metadata().name(i), type, klass));
        }

        return setValue(i, Codec.<Set<T>>getCodec(type).decompose(v));
    }

    public <T> BoundStatement setSet(String name, Set<T> v) {
        return setSet(metadata().getIdx(name), v);
    }

    private Columns metadata() {
        return statement.metadata;
    }

    private BoundStatement setValue(int i, ByteBuffer value) {
        ByteBuffer previous = values[i];
        values[i] = value;

        if (previous == null)
            remaining--;
        return this;
    }
}
