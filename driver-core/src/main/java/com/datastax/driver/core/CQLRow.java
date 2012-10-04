package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.transport.Codec;

import org.apache.cassandra.db.marshal.*;

/**
 * A CQL Row returned in a {@link ResultSet}.
 */
public class CQLRow {

    private final Columns metadata;
    private final List<ByteBuffer> data;

    private CQLRow(Columns metadata, List<ByteBuffer> data) {
        this.metadata = metadata;
        this.data = data;
    }

    static CQLRow fromData(Columns metadata, List<ByteBuffer> data) {
        if (data == null)
            return null;

        return new CQLRow(metadata, data);
    }

    /**
     * The columns contained in this CQLRow.
     *
     * @return the columns contained in this CQLRow.
     */
    public Columns columns() {
        return metadata;
    }

    /**
     * Returns whether the {@code i}th value of this row is NULL.
     *
     * @param i the index of the column to check.
     * @return whether the {@code i}th value of this row is NULL.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     */
    public boolean isNull(int i) {
        metadata.checkBounds(i);
        return data.get(i) != null;
    }

    /**
     * Returns whether the value for column {@code name} in this row is NULL.
     *
     * @param name the name of the column to check.
     * @return whether the value of column {@code name} is NULL.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     */
    public boolean isNull(String name) {
        return isNull(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a boolean.
     *
     * @param i the index of the column to retrieve.
     * @return the boolean value of the {@code i}th column in this row. If the
     * value is NULL, {@code false} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type BOOLEAN.
     */
    public boolean getBool(int i) {
        metadata.checkType(i, DataType.Native.BOOLEAN);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return false;

        return BooleanType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a boolean.
     *
     * @param name the name of the column to retrieve.
     * @return the boolean value of column {@code name}. If the value is NULL,
     * {@code false} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type BOOLEAN.
     */
    public boolean getBool(String name) {
        return getBool(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has an integer.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as an integer. If the
     * value is NULL, {@code 0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type INT.
     */
    public int getInt(int i) {
        metadata.checkType(i, DataType.Native.INT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0;

        return Int32Type.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has an integer.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as an integer. If the value is NULL,
     * {@code 0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type INT.
     */
    public int getInt(String name) {
        return getInt(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a long.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a long. If the
     * value is NULL, {@code 0L} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} type is not one of: BIGINT, TIMESTAMP,
     * INT or COUNTER.
     */
    public long getLong(int i) {
        DataType type = metadata.checkType(i, DataType.Native.BIGINT,
                                              DataType.Native.TIMESTAMP,
                                              DataType.Native.INT,
                                              DataType.Native.COUNTER);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        return type == DataType.Native.INT
             ? (long)Int32Type.instance.compose(value)
             : LongType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a long.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a long. If the value is NULL,
     * {@code 0L} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} type is not one of: BIGINT, TIMESTAMP,
     * INT or COUNTER.
     */
    public long getLong(String name) {
        return getLong(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a date.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a data. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type TIMESTAMP.
     */
    public Date getDate(int i) {
        metadata.checkType(i, DataType.Native.TIMESTAMP);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return DateType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a date.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a date. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type TIMESTAMP.
     */
    public Date getDate(String name) {
        return getDate(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a float.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a float. If the
     * value is NULL, {@code 0.0f} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type FLOAT.
     */
    public float getFloat(int i) {
        metadata.checkType(i, DataType.Native.FLOAT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0.0f;

        return FloatType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a float.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a float. If the value is NULL,
     * {@code 0.0f} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type FLOAT.
     */
    public float getFloat(String name) {
        return getFloat(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a double.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a double. If the
     * value is NULL, {@code 0.0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type
     * DOUBLE or FLOAT.
     */
    public double getDouble(int i) {
        DataType type = metadata.checkType(i, DataType.Native.DOUBLE,
                                              DataType.Native.FLOAT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0.0;

        return type == DataType.Native.FLOAT
             ? (double)FloatType.instance.compose(value)
             : DoubleType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a double.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a double. If the value is NULL,
     * {@code 0.0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type
     * DOUBLE or FLOAT.
     */
    public double getDouble(String name) {
        return getDouble(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a ByteBuffer.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a ByteBuffer. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     */
    public ByteBuffer getByteBuffer(int i) {
        metadata.checkBounds(i);

        ByteBuffer value = data.get(i);
        if (value == null)
            return null;

        return value.duplicate();
    }

    /**
     * Returns the value of column {@code name} has a ByteBuffer.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a ByteBuffer. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     */
    public ByteBuffer getByteBuffer(String name) {
        return getByteBuffer(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a byte array.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a byte array. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     */
    public byte[] getBytes(int i) {
        ByteBuffer bb = getByteBuffer(i);
        byte[] result = new byte[bb.remaining()];
        bb.get(result);
        return result;
    }

    /**
     * Returns the value of column {@code name} has a byte array.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a byte array. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     */
    public byte[] getBytes(String name) {
        return getBytes(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a string.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a string. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} type is none of:
     * VARCHAR, TEXT or ASCII.
     */
    public String getString(int i) {
        DataType type = metadata.checkType(i, DataType.Native.VARCHAR,
                                              DataType.Native.TEXT,
                                              DataType.Native.ASCII);

        ByteBuffer value = data.get(i);
        if (value == null)
            return null;

        return type == DataType.Native.ASCII
             ? AsciiType.instance.compose(value)
             : UTF8Type.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a string.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a string. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} type is none of:
     * VARCHAR, TEXT or ASCII.
     */
    public String getString(String name) {
        return getString(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a variable length integer.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a variable
     * length integer. If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type VARINT.
     */
    public BigInteger getVarInt(int i) {
        metadata.checkType(i, DataType.Native.VARINT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return IntegerType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a variable length integer.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a variable length integer.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type VARINT.
     */
    public BigInteger getVarInt(String name) {
        return getVarInt(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a variable length decimal.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a variable
     * length decimal. If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type DECIMAL.
     */
    public BigDecimal getDecimal(int i) {
        metadata.checkType(i, DataType.Native.DECIMAL);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return DecimalType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a variable length decimal.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a variable length decimal.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type DECIMAL.
     */
    public BigDecimal getDecimal(String name) {
        return getDecimal(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a UUID.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a UUID.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not of type UUID
     * or TIMEUUID.
     */
    public UUID getUUID(int i) {
        DataType type = metadata.checkType(i, DataType.Native.UUID, DataType.Native.TIMEUUID);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return type == DataType.Native.UUID
             ? UUIDType.instance.compose(value)
             : TimeUUIDType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has a UUID.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a UUID.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type
     * UUID or TIMEUUID.
     */
    public UUID getUUID(String name) {
        return getUUID(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a list.
     *
     * @param i the index of the column to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th column in this row as a list of
     * {@code elementsClass} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set).
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not a list or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        // TODO: this is not as flexible as the methods above. For instance,
        // with a list<int>, one cannot ask for getList(i, Long.class). We
        // might want to improve that, though that reach into the
        // ListType.compose() method.

        DataType type = metadata.type(i);
        if (!(type instanceof DataType.Collection.List))
            throw new InvalidTypeException(String.format("Column %s is not of list type", metadata.name(i)));

        DataType.Native eltType = (DataType.Native)((DataType.Collection.List)type).getElementsType();
        if (!Codec.isCompatible(eltType, elementsClass))
            throw new InvalidTypeException(String.format("Column %s is a %s, cannot be retrieve as a list of %s", metadata.name(i), type, elementsClass));

        ByteBuffer value = data.get(i);
        if (value == null)
            return Collections.<T>emptyList();

        // TODO: we could avoid the getCodec call if we kept a reference to the original message.
        return (List<T>)Codec.getCodec(type).compose(value);
    }

    /**
     * Returns the value of column {@code name} has a list.
     *
     * @param name the name of the column to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th column in this row as a list of
     * {@code elementsClass} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set).
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is not a list or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return getList(metadata.getIdx(name), elementsClass);
    }

    /**
     * Returns the {@code i}th value of this row has a set.
     *
     * @param i the index of the column to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th column in this row as a set of
     * {@code elementsClass} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set).
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not a set or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        DataType type = metadata.type(i);
        if (!(type instanceof DataType.Collection.Set))
            throw new InvalidTypeException(String.format("Column %s is not of set type", metadata.name(i)));

        DataType.Native eltType = (DataType.Native)((DataType.Collection.Set)type).getElementsType();
        if (!Codec.isCompatible(eltType, elementsClass))
            throw new InvalidTypeException(String.format("Column %s is a %s, cannot be retrieve as a set of %s", metadata.name(i), type, elementsClass));

        ByteBuffer value = data.get(i);
        if (value == null)
            return Collections.<T>emptySet();

        return (Set<T>)Codec.getCodec(type).compose(value);
    }

    /**
     * Returns the value of column {@code name} has a set.
     *
     * @param name the name of the column to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th column in this row as a set of
     * {@code elementsClass} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set).
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is not a set or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return getSet(metadata.getIdx(name), elementsClass);
    }

    /**
     * Returns the {@code i}th value of this row has a map.
     *
     * @param i the index of the column to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th column in this row as a map of
     * {@code keysClass} to {@code valuesClass} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set).
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().count()}.
     * @throws InvalidTypeException if column {@code i} is not a map, if its
     * keys are not of class {@code keysClass} or if its values are not of
     * class {@code valuesClass}.
     */
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        DataType type = metadata.type(i);
        if (!(type instanceof DataType.Collection.Map))
            throw new InvalidTypeException(String.format("Column %s is not of map type", metadata.name(i)));

        DataType.Collection.Map mapType = (DataType.Collection.Map)type;
        DataType.Native keysType = (DataType.Native)mapType.getKeysType();
        DataType.Native valuesType = (DataType.Native)mapType.getValuesType();
        if (!Codec.isCompatible(keysType, keysClass) || !Codec.isCompatible(valuesType, valuesClass))
            throw new InvalidTypeException(String.format("Column %s is a %s, cannot be retrieve as a map of %s -> %s", metadata.name(i), type, keysClass, valuesClass));

        ByteBuffer value = data.get(i);
        if (value == null)
            return Collections.<K, V>emptyMap();

        return (Map<K, V>)Codec.getCodec(type).compose(value);
    }

    /**
     * Returns the value of column {@code name} has a map.
     *
     * @param name the name of the column to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th column in this row as a map of
     * {@code keysClass} to {@code valuesClass} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set).
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is not a map, if its
     * keys are not of class {@code keysClass} or if its values are not of
     * class {@code valuesClass}.
     */
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(metadata.getIdx(name), keysClass, valuesClass);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CQLRow[");
        for (int i = 0; i < metadata.count(); i++) {
            if (i != 0)
                sb.append(", ");
            ByteBuffer bb = data.get(i);
            if (bb == null)
                sb.append("NULL");
            else
                sb.append(Codec.getCodec(metadata.type(i)).getString(bb));
        }
        sb.append("]");
        return sb.toString();
    }
}
