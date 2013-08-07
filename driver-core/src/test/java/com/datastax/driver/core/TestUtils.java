/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.math.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A number of static fields/methods handy for tests.
 */
public abstract class TestUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static final String CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }";
    public static final String CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : '%s', %s }";

    public static final String SIMPLE_KEYSPACE = "ks";
    public static final String SIMPLE_TABLE = "test";

    public static final String CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)";

    public static final String INSERT_FORMAT = "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)";
    public static final String SELECT_ALL_FORMAT = "SELECT * FROM %s";

    public static BoundStatement setBoundValue(BoundStatement bs, String name, DataType type, Object value) {
        switch (type.getName()) {
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
            case LIST:
                bs.setList(name, (List)value);
                break;
            case SET:
                bs.setSet(name, (Set)value);
                break;
            case MAP:
                bs.setMap(name, (Map)value);
                break;
            default:
                throw new RuntimeException("Missing handling of " + type);
        }
        return bs;
    }

    public static Object getValue(Row row, String name, DataType type) {
        switch (type.getName()) {
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
            case LIST:
                return row.getList(name, type.getTypeArguments().get(0).asJavaClass());
            case SET:
                return row.getSet(name, type.getTypeArguments().get(0).asJavaClass());
            case MAP:
                return row.getMap(name, type.getTypeArguments().get(0).asJavaClass(), type.getTypeArguments().get(1).asJavaClass());
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Always return the "same" value for each type
    public static Object getFixedValue(final DataType type) {
        try {
            switch (type.getName()) {
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
                case LIST:
                    return new ArrayList(){{ add(getFixedValue(type.getTypeArguments().get(0))); }};
                case SET:
                    return new HashSet(){{ add(getFixedValue(type.getTypeArguments().get(0))); }};
                case MAP:
                    return new HashMap(){{ put(getFixedValue(type.getTypeArguments().get(0)), getFixedValue(type.getTypeArguments().get(1))); }};
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Always return the "same" value for each type
    public static Object getFixedValue2(final DataType type) {
        try {
            switch (type.getName()) {
                case ASCII:
                    return "A different ascii string";
                case BIGINT:
                    return Long.MAX_VALUE;
                case BLOB:
                    ByteBuffer bb = ByteBuffer.allocate(64);
                    bb.putInt(0xCAFE);
                    bb.putShort((short) 3);
                    bb.putShort((short) 45);
                    return bb;
                case BOOLEAN:
                    return false;
                case COUNTER:
                    throw new UnsupportedOperationException("Cannot 'getSomeValue' for counters");
                case DECIMAL:
                    return new BigDecimal("12.3E+7");
                case DOUBLE:
                    return Double.POSITIVE_INFINITY;
                case FLOAT:
                    return Float.POSITIVE_INFINITY;
                case INET:
                    return InetAddress.getByName("123.123.123.123");
                case INT:
                    return Integer.MAX_VALUE;
                case TEXT:
                    return "résumé";
                case TIMESTAMP:
                    return new Date(872835240000L);
                case UUID:
                    return UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00");
                case VARCHAR:
                    return "A different varchar résumé";
                case VARINT:
                    return new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000");
                case TIMEUUID:
                    return UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66");
                case LIST:
                    return new ArrayList(){{ add(getFixedValue2(type.getTypeArguments().get(0))); }};
                case SET:
                    return new HashSet(){{ add(getFixedValue2(type.getTypeArguments().get(0))); }};
                case MAP:
                    return new HashMap(){{ put(getFixedValue2(type.getTypeArguments().get(0)), getFixedValue2(type.getTypeArguments().get(1))); }};
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Missing handling of " + type);
    }

    // Wait for a node to be up and running
    // This is used because there is some delay between when a node has been
    // added through ccm and when it's actually available for querying
    public static void waitFor(String node, Cluster cluster) {
        waitFor(node, cluster, 30, false, false);
    }

    public static void waitFor(String node, Cluster cluster, int maxTry) {
        waitFor(node, cluster, maxTry, false, false);
    }

    public static void waitForDown(String node, Cluster cluster) {
        waitFor(node, cluster, 30, true, false);
    }

    public static void waitForDownWithWait(String node, Cluster cluster, int waitTime) {
        waitFor(node, cluster, 30, true, false);

        // FIXME: Once stop() works, remove this line
        try {
            Thread.sleep(waitTime * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void waitForDown(String node, Cluster cluster, int maxTry) {
        waitFor(node, cluster, maxTry, true, false);
    }

    public static void waitForDecommission(String node, Cluster cluster) {
        waitFor(node, cluster, 30, true, true);
    }

    public static void waitForDecommission(String node, Cluster cluster, int maxTry) {
        waitFor(node, cluster, maxTry, true, true);
    }

    private static void waitFor(String node, Cluster cluster, int maxTry, boolean waitForDead, boolean waitForOut) {
        if (waitForDead || waitForOut)
            if (waitForDead)
                logger.info("Waiting for stopped node: " + node);
            else if (waitForOut)
                logger.info("Waiting for decommissioned node: " + node);
        else
            logger.info("Waiting for upcoming node: " + node);

        // In the case where the we've killed the last node in the cluster, if we haven't
        // tried doing an actual query, the driver won't realize that last node is dead until
        // keep alive kicks in, but that's a fairly long time. So we cheat and trigger a force
        // the detection by forcing a request.
        if (waitForDead || waitForOut)
            cluster.manager.submitSchemaRefresh(null, null);

        InetAddress address;
        try {
             address = InetAddress.getByName(node);
        } catch (Exception e) {
            // That's a problem but that's not *our* problem
            return;
        }

        Metadata metadata = cluster.getMetadata();
        for (int i = 0; i < maxTry; ++i) {
            for (Host host : metadata.getAllHosts()) {
                if (host.getAddress().equals(address) && testHost(host, waitForDead))
                    return;
            }
            try { Thread.sleep(1000); } catch (Exception e) {}
        }

        for (Host host : metadata.getAllHosts()) {
            if (host.getAddress().equals(address)) {
                if (testHost(host, waitForDead)) {
                    return;
                } else {
                    // logging it because this give use the timestamp of when this happens
                    logger.info(node + " is not " + (waitForDead ? "DOWN" : "UP") + " after " + maxTry + "s");
                    throw new IllegalStateException(node + " is not " + (waitForDead ? "DOWN" : "UP") + " after " + maxTry + "s");
                }
            }
        }

        if (waitForOut){
            return;
        } else {
            logger.info(node + " is not part of the cluster after " + maxTry + "s");
            throw new IllegalStateException(node + " is not part of the cluster after " + maxTry + "s");
        }
    }

    private static boolean testHost(Host host, boolean testForDown) {
        return testForDown ? !host.isUp() : host.isUp();
    }

    // Check for all nodes to come online for up to 30 seconds
    public static void waitForAllNodesToComeOnline(Session session, int totalNodes) {
        int maxTry = 30;
        for (int i = 0; i < maxTry; ++i) {
            List<Row> rs = session.execute("SELECT * from system.peers").all();

            // Don't count yourself in the peers list
            if (rs.size() == totalNodes - 1) {
                return;
            }

            // Throttle schema polling
            try { Thread.sleep(1000); } catch (Exception e) {}
        }

        // Throw exception if not all nodes came online
        throw new RuntimeException(String.format("Not all nodes came online within %s seconds.", maxTry));
    }

    // Check for a schema agreement with all nodes for up to 30 seconds
    public static void waitForSchemaAgreement(Session session) {
        int maxTry = 30;
        UUID schemaVersion;
        for (int i = 0; i < maxTry; ++i) {
            schemaVersion = null;
            List<Row> rs = session.execute("SELECT * from system.peers").all();

            // Track disagreements
            boolean schemaDisagreement = false;

            for (Row row : rs) {
                // Save the first schemaVersion
                if (schemaVersion == null) {
                    schemaVersion = row.getUUID("schema_version");
                    continue;
                }

                // Compare all succeeding schemaVersions to the first schemaVersion
                if (!schemaVersion.equals(row.getUUID("schema_version"))) {
                    schemaDisagreement = true;
                    break;
                }
            }

            // Exit without an exception when a schema agreement has been reached
            if (!schemaDisagreement)
                return;

            // Throttle schema polling
            try { Thread.sleep(1000); } catch (Exception e) {}
        }

        // Throw exception if schema agreement is never reached
        throw new RuntimeException(String.format("Schema agreement not reached within %s seconds.", maxTry));
    }
}
