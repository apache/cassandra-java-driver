package com.datastax.driver.core;

import java.math.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * A number of static fields/methods handy for tests.
 */
public abstract class TestUtils {

    public static final String CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }";
    public static final String CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : '%s', %s }";

    public static final String SIMPLE_KEYSPACE = "ks";

    public static BoundStatement setBoundValue(BoundStatement bs, String name, DataType type, Object value) {
        if (type.isCollection()) {
            switch (type.asCollection().getKind()) {
                case LIST:
                    bs.setList(name, (List)value);
                    break;
                case SET:
                    bs.setSet(name, (Set)value);
                    break;
                case MAP:
                    bs.setMap(name, (Map)value);
                    break;
            }

        } else {
            switch (type.asNative()) {
                case ASCII:
                    bs.setString(name, (String)value);
                    break;
                case BIGINT:
                    bs.setLong(name, (Long)value);
                    break;
                case BLOB:
                    bs.setBytes(name, (ByteBuffer)value);
                    break;
                case BOOLEAN:
                    bs.setBool(name, (Boolean)value);
                    break;
                case COUNTER:
                    // Just a no-op, we shouldn't handle counters the same way than other types
                    break;
                case DECIMAL:
                    bs.setDecimal(name, (BigDecimal)value);
                    break;
                case DOUBLE:
                    bs.setDouble(name, (Double)value);
                    break;
                case FLOAT:
                    bs.setFloat(name, (Float)value);
                    break;
                case INET:
                    bs.setInet(name, (InetAddress)value);
                    break;
                case INT:
                    bs.setInt(name, (Integer)value);
                    break;
                case TEXT:
                    bs.setString(name, (String)value);
                    break;
                case TIMESTAMP:
                    bs.setDate(name, (Date)value);
                    break;
                case UUID:
                    bs.setUUID(name, (UUID)value);
                    break;
                case VARCHAR:
                    bs.setString(name, (String)value);
                    break;
                case VARINT:
                    bs.setVarint(name, (BigInteger)value);
                    break;
                case TIMEUUID:
                    bs.setUUID(name, (UUID)value);
                    break;
                default:
                    throw new RuntimeException("Missing handling of " + type);
            }
        }
        return bs;
    }

    public static Object getValue(CQLRow row, String name, DataType type) {
        if (type.isCollection()) {
            switch (type.asCollection().getKind()) {
                case LIST:
                    return row.getList(name, classOf(((DataType.Collection.List)type).getElementsType()));
                case SET:
                    return row.getSet(name, classOf(((DataType.Collection.Set)type).getElementsType()));
                case MAP:
                    DataType.Collection.Map mt = (DataType.Collection.Map)type;
                    return row.getMap(name, classOf(mt.getKeysType()), classOf(mt.getValuesType()));
            }
        } else {
            switch (type.asNative()) {
                case ASCII:
                    return row.getString(name);
                case BIGINT:
                    return row.getLong(name);
                case BLOB:
                    return row.getBytes(name);
                case BOOLEAN:
                    return row.getBool(name);
                case COUNTER:
                    return row.getLong(name);
                case DECIMAL:
                    return row.getDecimal(name);
                case DOUBLE:
                    return row.getDouble(name);
                case FLOAT:
                    return row.getFloat(name);
                case INET:
                    return row.getInet(name);
                case INT:
                    return row.getInt(name);
                case TEXT:
                    return row.getString(name);
                case TIMESTAMP:
                    return row.getDate(name);
                case UUID:
                    return row.getUUID(name);
                case VARCHAR:
                    return row.getString(name);
                case VARINT:
                    return row.getVarint(name);
                case TIMEUUID:
                    return row.getUUID(name);
            }
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    private static Class classOf(DataType type) {
        assert !type.isCollection();

        switch (type.asNative()) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return String.class;
            case BIGINT:
            case COUNTER:
                return Long.class;
            case BLOB:
                return ByteBuffer.class;
            case BOOLEAN:
                return Boolean.class;
            case DECIMAL:
                return BigDecimal.class;
            case DOUBLE:
                return Double.class;
            case FLOAT:
                return Float.class;
            case INET:
                return InetAddress.class;
            case INT:
                return Integer.class;
            case TIMESTAMP:
                return Date.class;
            case UUID:
            case TIMEUUID:
                return UUID.class;
            case VARINT:
                return BigInteger.class;
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Always return the "same" value for each type
    public static Object getFixedValue(final DataType type) {
        try {
            if (type.isCollection()) {
                switch (type.asCollection().getKind()) {
                    case LIST:
                        return new ArrayList(){{ add(getFixedValue(((DataType.Collection.List)type).getElementsType())); }};
                    case SET:
                        return new HashSet(){{ add(getFixedValue(((DataType.Collection.Set)type).getElementsType())); }};
                    case MAP:
                        final DataType.Collection.Map mt = (DataType.Collection.Map)type;
                        return new HashMap(){{ put(getFixedValue(mt.getKeysType()), getFixedValue(mt.getValuesType())); }};
                }
            } else {
                switch (type.asNative()) {
                    case ASCII:
                        return "An ascii string";
                    case BIGINT:
                        return 42L;
                    case BLOB:
                        return ByteBuffer.wrap(new byte[]{ (byte)4, (byte)12, (byte)1 });
                    case BOOLEAN:
                        return true;
                    case COUNTER:
                        throw new UnsupportedOperationException("Cannot 'getSomeValue' for counters");
                    case DECIMAL:
                        return new BigDecimal("3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679");
                    case DOUBLE:
                        return 3.142519;
                    case FLOAT:
                        return 3.142519f;
                    case INET:
                        return InetAddress.getByAddress(new byte[]{(byte)127, (byte)0, (byte)0, (byte)1});
                    case INT:
                        return 24;
                    case TEXT:
                        return "A text string";
                    case TIMESTAMP:
                        return new Date(1352288289L);
                    case UUID:
                        return UUID.fromString("087E9967-CCDC-4A9B-9036-05930140A41B");
                    case VARCHAR:
                        return "A varchar string";
                    case VARINT:
                        return new BigInteger("123456789012345678901234567890");
                    case TIMEUUID:
                        return UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Wait for a node to be up and running
    // This is used because there is some delay between when a node has been
    // added through ccm and when it's actually available for querying
    public static void waitFor(String node, Cluster cluster, int maxTry) {
        InetAddress address;
        try {
             address = InetAddress.getByName(node);
        } catch (Exception e) {
            // That's a problem but that's not *our* problem
            return;
        }

        ClusterMetadata metadata = cluster.getMetadata();
        for (int i = 0; i < maxTry; ++i) {
            for (Host host : metadata.getAllHosts()) {
                if (host.getAddress().equals(address) && host.getMonitor().isUp())
                    return;
            }
            try { Thread.sleep(1000); } catch (Exception e) {}
        }

        for (Host host : metadata.getAllHosts()) {
            if (host.getAddress().equals(address)) {
                if (host.getMonitor().isUp())
                    return;
                else
                    throw new IllegalStateException(node + " is part of the cluster but is not UP after " + maxTry + "s");
            }
        }
        throw new IllegalStateException(node + " is not part of the cluster after " + maxTry + "s");
    }
}
