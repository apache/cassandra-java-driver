/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;

public interface CCMAccess extends Closeable {

    enum Workload {
        cassandra,
        solr,
        hadoop,
        spark,
        cfs,
        graph
    }

    // Inspection methods

    /**
     * @return The CCM cluster name.
     */
    String getClusterName();

    /**
     * Returns the Cassandra version of this CCM cluster.
     * <p/>
     * By default the version is equal to {@link CCMBridge#getCassandraVersion()}.
     *
     * @return The version of this CCM cluster.
     */
    VersionNumber getVersion();

    /**
     * @return The config directory for this CCM cluster.
     */
    File getCcmDir();

    /**
     * @return The cluster directory for this CCM cluster.
     */
    File getClusterDir();

    /**
     * @param n the node number, starting with 1.
     * @return The node directory for this CCM cluster.
     */
    File getNodeDir(int n);

    /**
     * @param n the node number, starting with 1.
     * @return The node config directory for this CCM cluster.
     */
    File getNodeConfDir(int n);

    /**
     * @return The storage port for this CCM cluster.
     */
    int getStoragePort();

    /**
     * @return The thrift port for this CCM cluster.
     */
    int getThriftPort();

    /**
     * @return The binary port for this CCM cluster.
     */
    int getBinaryPort();

    /**
     * Signals that logs for this CCM cluster should be kept after the cluster is stopped.
     */
    void setKeepLogs(boolean keepLogs);

    /**
     * Returns the address of the {@code nth} host in the CCM cluster (counting from 1, i.e.,
     * {@code addressOfNode(1)} returns the address of the first node.
     * <p/>
     * In multi-DC setups, nodes are numbered in ascending order of their datacenter number.
     * E.g. with 2 DCs and 3 nodes in each DC, the first node in DC 2 is number 4.
     *
     * @return the address of the {@code nth} host in the cluster.
     */
    InetSocketAddress addressOfNode(int n);

    // Methods altering the whole cluster

    /**
     * Starts the cluster.
     */
    void start();

    /**
     * Stops the cluster.
     */
    void stop();

    /**
     * Aggressively stops the cluster.
     */
    void forceStop();

    /**
     * Alias for {@link #stop()}.
     */
    @Override
    void close();

    /**
     * Removes this CCM cluster and deletes all of its files.
     */
    void remove();

    /**
     * Updates the config files for all nodes in the CCM cluster.
     */
    void updateConfig(Map<String, Object> configs);

    /**
     * Updates the DSE config files for all nodes in the CCM cluster.
     */
    void updateDSEConfig(Map<String, Object> configs);

    /**
     * Checks for errors in the logs of all nodes in the cluster.
     */
    String checkForErrors();

    // Methods altering nodes

    /**
     * Starts the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void start(int n);

    /**
     * Stops the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void stop(int n);

    /**
     * Aggressively stops the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void forceStop(int n);

    /**
     * Removes the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void remove(int n);

    /**
     * Adds the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void add(int n);

    /**
     * Adds the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void add(int dc, int n);

    /**
     * Decommissions the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void decommission(int n);

    /**
     * Updates the {@code nth} host's config file in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void updateNodeConfig(int n, String key, Object value);

    /**
     * Updates the {@code nth} host's config file in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void updateNodeConfig(int n, Map<String, Object> configs);

    /**
     * Updates the {@code nth} host's dse config file in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void updateDSENodeConfig(int n, String key, Object value);

    /**
     * Updates the {@code nth} host's dse config file in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void updateDSENodeConfig(int n, Map<String, Object> configs);

    /**
     * Sets the workload(s) for the {@code nth} host in the CCM cluster.
     *
     * @param n the node number (starting from 1).
     */
    void setWorkload(int n, Workload... workload);

    // Methods blocking until nodes are up or down

    /**
     * Waits for a host to be up by pinging the TCP socket directly, without using the Java driver's API.
     */
    void waitForUp(int node);

    /**
     * Waits for a host to be down by pinging the TCP socket directly, without using the Java driver's API.
     */
    void waitForDown(int node);

}
