package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.marshal.*;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A CQL Row returned in a {@link ResultSet}.
 * <p>
 * The values of a CQLRow can be retrieve by either index or name. When
 * setting them by name, names follow the case insensitivity rules explained in
 * {@link ColumnDefinitions}.
 */
public class Row {

    private final ColumnDefinitions metadata;
    private final List<ByteBuffer> data;

    private Row(ColumnDefinitions metadata, List<ByteBuffer> data) {
        this.metadata = metadata;
        this.data = data;
    }

    static Row fromData(ColumnDefinitions metadata, List<ByteBuffer> data) {
        if (data == null)
            return null;

        return new Row(metadata, data);
    }

    /**
     * The columns contained in this Row.
     *
     * @return the columns contained in this Row.
     */
    public ColumnDefinitions getColumnDefinitions() {
        return metadata;
    }

    /**
     * Returns whether the {@code i}th value of this row is NULL.
     *
     * @param i the index of the column to check.
     * @return whether the {@code i}th value of this row is NULL.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     */
    public boolean isNull(int i) {
        metadata.checkBounds(i);
        return data.get(i) == null;
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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BOOLEAN.
     */
    public boolean getBool(int i) {
        metadata.checkType(i, DataType.Name.BOOLEAN);

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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type INT.
     */
    public int getInt(int i) {
        metadata.checkType(i, DataType.Name.INT);

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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BIGINT or COUNTER.
     */
    public long getLong(int i) {
        metadata.checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        return LongType.instance.compose(value);
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
     * @throws InvalidTypeException if column {@code i} is not of type BIGINT or COUNTER.
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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type TIMESTAMP.
     */
    public Date getDate(int i) {
        metadata.checkType(i, DataType.Name.TIMESTAMP);

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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type FLOAT.
     */
    public float getFloat(int i) {
        metadata.checkType(i, DataType.Name.FLOAT);

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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type DOUBLE.
     */
    public double getDouble(int i) {
        metadata.checkType(i, DataType.Name.DOUBLE);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0.0;

        return DoubleType.instance.compose(value);
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
     * @throws InvalidTypeException if column {@code name} is not of type DOUBLE.
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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     */
    public ByteBuffer getBytesUnsafe(int i) {
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
    public ByteBuffer getBytesUnsafe(String name) {
        return getBytesUnsafe(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a byte array.
     * <p>
     * Note that this method validate that the colum is of type BLOB. If you want to retrieve
     * the bytes for any type of columns, use {@link #getBytesUnsafe(int)} instead.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a byte array. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} type is not of type BLOB.
     */
    public ByteBuffer getBytes(int i) {
        metadata.checkType(i, DataType.Name.BLOB);
        return getBytesUnsafe(i);
    }

    /**
     * Returns the value of column {@code name} has a byte array.
     * <p>
     * Note that this method validate that the colum is of type BLOB. If you want to retrieve
     * the bytes for any type of columns, use {@link #getBytesUnsafe(String)} instead.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a byte array. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} type is not of type BLOB.
     */
    public ByteBuffer getBytes(String name) {
        return getBytes(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a string.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a string. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} type is none of:
     * VARCHAR, TEXT or ASCII.
     */
    public String getString(int i) {
        DataType.Name type = metadata.checkType(i, DataType.Name.VARCHAR,
                                                   DataType.Name.TEXT,
                                                   DataType.Name.ASCII);

        ByteBuffer value = data.get(i);
        if (value == null)
            return null;

        return type == DataType.Name.ASCII
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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type VARINT.
     */
    public BigInteger getVarint(int i) {
        metadata.checkType(i, DataType.Name.VARINT);

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
    public BigInteger getVarint(String name) {
        return getVarint(metadata.getIdx(name));
    }

    /**
     * Returns the {@code i}th value of this row has a variable length decimal.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a variable
     * length decimal. If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type DECIMAL.
     */
    public BigDecimal getDecimal(int i) {
        metadata.checkType(i, DataType.Name.DECIMAL);

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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type UUID
     * or TIMEUUID.
     */
    public UUID getUUID(int i) {
        DataType.Name type = metadata.checkType(i, DataType.Name.UUID, DataType.Name.TIMEUUID);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return type == DataType.Name.UUID
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
     * Returns the {@code i}th value of this row has an InetAddress.
     *
     * @param i the index of the column to retrieve.
     * @return the value of the {@code i}th column in this row as an InetAddress.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type INET.
     */
    public InetAddress getInet(int i) {
        metadata.checkType(i, DataType.Name.INET);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return InetAddressType.instance.compose(value);
    }

    /**
     * Returns the value of column {@code name} has an InetAddress.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as an InetAddress.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type
     * INET.
     */
    public InetAddress getInet(String name) {
        return getInet(metadata.getIdx(name));
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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not a list or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is not of list type", metadata.getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a list of %s (CQL type %s), cannot be retrieve as a list of %s", metadata.getName(i), expectedClass, type, elementsClass));

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
     * @throws InvalidTypeException if column {@code name} is not a list or if its
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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not a set or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is not of set type", metadata.getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a set of %s (CQL type %s), cannot be retrieve as a set of %s", metadata.getName(i), expectedClass, type, elementsClass));

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
     * @throws InvalidTypeException if column {@code name} is not a set or if its
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
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not a map, if its
     * keys are not of class {@code keysClass} or if its values are not of
     * class {@code valuesClass}.
     */
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.MAP)
            throw new InvalidTypeException(String.format("Column %s is not of map type", metadata.getName(i)));

        Class<?> expectedKeysClass = type.getTypeArguments().get(0).getName().javaType;
        Class<?> expectedValuesClass = type.getTypeArguments().get(1).getName().javaType;
        if (!keysClass.isAssignableFrom(expectedKeysClass) || !valuesClass.isAssignableFrom(expectedValuesClass))
            throw new InvalidTypeException(String.format("Column %s is a map of %s->%s (CQL type %s), cannot be retrieve as a map of %s->%s", metadata.getName(i), expectedKeysClass, expectedValuesClass, type, keysClass, valuesClass));

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
     * @throws InvalidTypeException if column {@code name} is not a map, if its
     * keys are not of class {@code keysClass} or if its values are not of
     * class {@code valuesClass}.
     */
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(metadata.getIdx(name), keysClass, valuesClass);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Row[");
        for (int i = 0; i < metadata.size(); i++) {
            if (i != 0)
                sb.append(", ");
            ByteBuffer bb = data.get(i);
            if (bb == null)
                sb.append("NULL");
            else
                sb.append(Codec.getCodec(metadata.getType(i)).getString(bb));
        }
        sb.append("]");
        return sb.toString();
    }
}
