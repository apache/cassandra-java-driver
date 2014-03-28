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
package com.datastax.driver.core.policies;

import java.net.InetSocketAddress;

import com.datastax.driver.core.Cluster;

/**
 * Translates IP addresses received from Cassandra nodes into locally queriable
 * addresses.
 * <p>
 * The driver auto-detect new Cassandra nodes added to the cluster through server
 * side pushed notifications and through checking the system tables. For each
 * node, the address the driver will receive will correspond to the address set as
 * {@code rpc_address} in the node yaml file. In most case, this is the correct
 * address to use by the driver and that is what is used by default. However,
 * sometimes the addresses received through this mechanism will either not be
 * reachable directly by the driver or should not be the prefered address to use
 * to reach the node (for instance, the {@code rpc_address} set on Cassandra nodes
 * might be a private IP, but some clients  may have to use a public IP, or
 * pass by a router to reach that node). This interface allows to deal with
 * such cases, by allowing to translate an address as sent by a Cassandra node
 * to another address to be used by the driver for connection.
 * <p>
 * Please note that the contact points addresses provided while creating the
 * {@link Cluster} instance are not "tanslated", only IP address retrieve from or sent
 * by Cassandra nodes to the driver are.
 */
public interface AddressTranslater {

    /**
     * Translates a Cassandra {@code rpc_address} to another address if necessary.
     *
     * @param address the address of a node as returned by Cassandra. Note that
     * if the {@code rpc_address} of a node has been configured to {@code 0.0.0.0}
     * server side, then the provided address will be the node {@code listen_address},
     * *not* {@code 0.0.0.0}. Also note that the port for {@code InetSocketAddress}
     * will always be the one set at Cluster construction time (9042 by default).
     * @return the address the driver should actually use to connect to the node
     * designated by {@code address}. If the return is {@code null}, then {@code
     * address} will be used by the driver (it is thus equivalent to returing
     * {@code address} directly)
     */
    public InetSocketAddress translate(InetSocketAddress address);
}
