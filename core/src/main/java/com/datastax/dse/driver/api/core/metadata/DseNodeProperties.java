/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;

/** The keys for the additional DSE-specific properties stored in {@link Node#getExtras()}. */
public class DseNodeProperties {

  /**
   * The DSE version that the node is running.
   *
   * <p>The associated value in {@link Node#getExtras()} is a {@link Version}).
   */
  public static final String DSE_VERSION = "DSE_VERSION";

  /**
   * The value of the {@code server_id} field in the {@code peers} system table for this node.
   *
   * <p>This is the single identifier of the machine running a DSE instance. If DSE has been
   * configured with Multi-Instance, the {@code server_id} helps identifying the single physical
   * machine that runs the multiple DSE instances. If DSE is not configured with DSE Multi-Instance,
   * the {@code server_id} will be automatically set and be unique for each node.
   *
   * <p>This information is only available if connecting to a DSE 6.0+ node.
   *
   * <p>The associated value in {@link Node#getExtras()} is a {@code String}).
   *
   * @see <a
   *     href="http://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/multiInstance/multiInstanceTOC.html">DSE
   *     Multi-Instance (DSE Administrator Guide)</a>
   * @see <a
   *     href="http://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/config/configDseYaml.html#configDseYaml__dseMultiInstance">
   *     server_id (DSE Administrator Guide)</a>
   */
  public static final String SERVER_ID = "SERVER_ID";

  /**
   * The DSE workloads that the node is running.
   *
   * <p>This is based on the {@code workload} or {@code workloads} columns in {@code system.local}
   * and {@code system.peers}.
   *
   * <p>Workload labels may vary depending on the DSE version in use; e.g. DSE 5.1 may report two
   * distinct workloads: {@code Search} and {@code Analytics}, while DSE 5.0 would report a single
   * {@code SearchAnalytics} workload instead. It is up to users to deal with such discrepancies;
   * the driver simply returns the workload labels as reported by DSE, without any form of
   * pre-processing (with the exception of Graph in DSE 5.0, which is stored in a separate column,
   * but will be reported as {@code Graph} here).
   *
   * <p>The associated value in {@link Node#getExtras()} is an immutable {@code Set<String>}.
   */
  public static final String DSE_WORKLOADS = "DSE_WORKLOADS";

  /**
   * The port for the native transport connections on the DSE node.
   *
   * <p>The native transport port is {@code 9042} by default but can be changed on instances
   * requiring specific firewall configurations. This can be configured in the {@code
   * cassandra.yaml} configuration file under the {@code native_transport_port} property.
   *
   * <p>This information is only available if connecting the driver to a DSE 6.0+ node.
   *
   * <p>The associated value in {@link Node#getExtras()} is an {@code Integer}.
   */
  public static final String NATIVE_TRANSPORT_PORT = "NATIVE_TRANSPORT_PORT";

  /**
   * The port for the encrypted native transport connections on the DSE node.
   *
   * <p>In most scenarios enabling client communications in DSE will result in using a single port
   * that will only accept encrypted connections (by default the port {@code 9042} is reused since
   * unencrypted connections are not allowed).
   *
   * <p>However, it is possible to configure DSE to use both encrypted and a non-encrypted
   * communication ports with clients. In that case the port accepting encrypted connections will
   * differ from the non-encrypted one (see {@link #NATIVE_TRANSPORT_PORT}) and will be exposed via
   * this method.
   *
   * <p>This information is only available if connecting the driver to a DSE 6.0+ node.
   *
   * <p>The associated value in {@link Node#getExtras()} is an {@code Integer}.
   */
  public static final String NATIVE_TRANSPORT_PORT_SSL = "NATIVE_TRANSPORT_PORT_SSL";

  /**
   * The storage port used by the DSE node.
   *
   * <p>The storage port is used for internal communication between the DSE server nodes. This port
   * is never used by the driver.
   *
   * <p>This information is only available if connecting the driver to a DSE 6.0+ node.
   *
   * <p>The associated value in {@link Node#getExtras()} is an {@code Integer}.
   */
  public static final String STORAGE_PORT = "STORAGE_PORT";

  /**
   * The encrypted storage port used by the DSE node.
   *
   * <p>If inter-node encryption is enabled on the DSE cluster, nodes will communicate securely
   * between each other via this port. This port is never used by the driver.
   *
   * <p>This information is only available if connecting the driver to a DSE 6.0+ node.
   *
   * <p>The associated value in {@link Node#getExtras()} is an {@code Integer}.
   */
  public static final String STORAGE_PORT_SSL = "STORAGE_PORT_SSL";

  /**
   * The JMX port used by this node.
   *
   * <p>The JMX port can be configured in the {@code cassandra-env.sh} configuration file separately
   * on each node.
   *
   * <p>This information is only available if connecting the driver to a DSE 6.0+ node.
   *
   * <p>The associated value in {@link Node#getExtras()} is an {@code Integer}.
   */
  public static final String JMX_PORT = "JMX_PORT";
}
