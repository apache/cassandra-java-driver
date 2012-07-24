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
     * The columns contains in this CQLRow.
     *
     * @return the columns contained in this CQLRow.
     */
    public Columns columns() {
        return metadata;
    }

    public boolean isNull(int i) {
        metadata.checkBounds(i);
        return data.get(i) != null;
    }

    public boolean isNull(String name) {
        return isNull(metadata.getIdx(name));
    }

    /**
     */
    public boolean getBool(int i) {
        metadata.checkType(i, DataType.Native.BOOLEAN);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return false;

        return BooleanType.instance.compose(value);
    }

    public boolean getBool(String name) {
        return getBool(metadata.getIdx(name));
    }

    public int getInt(int i) {
        metadata.checkType(i, DataType.Native.INT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0;

        return Int32Type.instance.compose(value);
    }

    public int getInt(String name) {
        return getInt(metadata.getIdx(name));
    }

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

    public long getLong(String name) {
        return getLong(metadata.getIdx(name));
    }

    public Date getDate(int i) {
        metadata.checkType(i, DataType.Native.TIMESTAMP);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return DateType.instance.compose(value);
    }

    public Date getDate(String name) {
        return getDate(metadata.getIdx(name));
    }

    public float getFloat(int i) {
        metadata.checkType(i, DataType.Native.FLOAT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0.0f;

        return FloatType.instance.compose(value);
    }

    public float getFloat(String name) {
        return getFloat(metadata.getIdx(name));
    }

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

    public ByteBuffer getByteBuffer(int i) {
        metadata.checkBounds(i);

        ByteBuffer value = data.get(i);
        if (value == null)
            return null;

        return value.duplicate();
    }

    public ByteBuffer getByteBuffer(String name) {
        return getByteBuffer(metadata.getIdx(name));
    }

    public byte[] getBytes(int i) {
        ByteBuffer bb = getByteBuffer(i);
        byte[] result = new byte[bb.remaining()];
        bb.get(result);
        return result;
    }

    public byte[] getBytes(String name) {
        return getBytes(metadata.getIdx(name));
    }

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

    public String getString(String name) {
        return getString(metadata.getIdx(name));
    }

    public BigInteger getVarInt(int i) {
        metadata.checkType(i, DataType.Native.VARINT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return IntegerType.instance.compose(value);
    }

    public BigInteger getVarInt(String name) {
        return getVarInt(metadata.getIdx(name));
    }

    public BigDecimal getDecimal(int i) {
        metadata.checkType(i, DataType.Native.DECIMAL);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return DecimalType.instance.compose(value);
    }

    public BigDecimal getDecimal(String name) {
        return getDecimal(metadata.getIdx(name));
    }

    public UUID getUUID(int i) {
        DataType type = metadata.checkType(i, DataType.Native.UUID, DataType.Native.TIMEUUID);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return type == DataType.Native.UUID
             ? UUIDType.instance.compose(value)
             : TimeUUIDType.instance.compose(value);
    }

    public UUID getUUID(String name) {
        return getUUID(metadata.getIdx(name));
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
