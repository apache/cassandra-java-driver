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
     * @throws InvalidTypeException if columns {@code i} is not of type TIMESTAMP.
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
     * @throws InvalidTypeException if columns {@code name} is not of type TIMESTAMP.
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
     * @throws InvalidTypeException if columns {@code i} is not of type FLOAT.
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
     * @throws InvalidTypeException if columns {@code name} is not of type FLOAT.
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
     * @throws InvalidTypeException if columns {@code i} is not of type
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
     * @throws InvalidTypeException if columns {@code name} is not of type
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
     * @throws InvalidTypeException if columns {@code i} type is none of:
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
     * @throws InvalidTypeException if columns {@code name} type is none of:
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
     * @throws InvalidTypeException if columns {@code i} is not of type VARINT.
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
     * @throws InvalidTypeException if columns {@code name} is not of type VARINT.
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
     * @throws InvalidTypeException if columns {@code i} is not of type DECIMAL.
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
     * @throws InvalidTypeException if columns {@code name} is not of type DECIMAL.
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
     * @throws InvalidTypeException if columns {@code i} is not of type UUID
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
     * @throws InvalidTypeException if columns {@code name} is not of type
     * UUID or TIMEUUID.
     */
    public UUID getUUID(String name) {
        return getUUID(metadata.getIdx(name));
    }

    public <T> List<T> getList(int i, Class<T> elts) {
        // TODO
        return null;
    }

    public <T> List<T> getList(String name, Class<T> elts) {
        return getList(metadata.getIdx(name), elts);
    }

    public <T> Set<T> getSet(int i, Class<T> elts) {
        // TODO
        return null;
    }

    public <T> Set<T> getSet(String name, Class<T> elts) {
        return getSet(metadata.getIdx(name), elts);
    }

    public <K, V> Map<K, V> getMap(int i, Class<K> keys, Class<V> values) {
        // TODO
        return null;
    }

    public <K, V> Map<K, V> getMap(String name, Class<K> keys, Class<V> values) {
        return getMap(metadata.getIdx(name), keys, values);
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
