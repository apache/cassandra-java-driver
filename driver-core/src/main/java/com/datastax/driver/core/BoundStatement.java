package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

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

    public boolean isSet(int i) {
        metadata().checkBounds(i);
        return values[i] != null;
    }

    public boolean isSet(String name) {
        return isSet(metadata().getIdx(name));
    }

    public BoundStatement bind(Object... values) {
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
