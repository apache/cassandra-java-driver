/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DefaultSchemaQueriesFactory implements SchemaQueriesFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaQueriesFactory.class);

  protected final InternalDriverContext context;
  protected final String logPrefix;

  public DefaultSchemaQueriesFactory(InternalDriverContext context) {
    this.context = context;
    this.logPrefix = context.getSessionName();
  }

  @Override
  public SchemaQueries newInstance() {
    DriverChannel channel = context.getControlConnection().channel();
    if (channel == null || channel.closeFuture().isDone()) {
      return new HandlerSchemaQueries("Control channel not available, aborting schema refresh");
    }
    try {
      Node node =
          context
              .getMetadataManager()
              .getMetadata()
              .findNode(channel.getEndPoint())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Could not find control node metadata "
                              + channel.getEndPoint()
                              + ", aborting schema refresh"));
      return newInstance(node, channel);
    } catch (IllegalStateException ex) {
      return new HandlerSchemaQueries(ex.getMessage());
    }
  }

  protected SchemaQueries newInstance(Node node, DriverChannel channel) {

    DriverExecutionProfile config = context.getConfig().getDefaultProfile();

    Version dseVersion = (Version) node.getExtras().get(DseNodeProperties.DSE_VERSION);
    if (dseVersion != null) {
      dseVersion = dseVersion.nextStable();

      LOG.debug(
          "[{}] Sending schema queries to {} with DSE version {}", logPrefix, node, dseVersion);
      // 4.8 is the oldest version supported, which uses C* 2.1 schema
      if (dseVersion.compareTo(Version.V5_0_0) < 0) {
        return new Cassandra21SchemaQueries(channel, node, config, logPrefix);
      } else if (dseVersion.compareTo(Version.V6_7_0) < 0) {
        // 5.0 - 6.7 uses C* 3.0 schema
        return new Cassandra3SchemaQueries(channel, node, config, logPrefix);
      } else if (dseVersion.compareTo(Version.V6_8_0) < 0) {
        // 6.7 uses C* 4.0 schema
        return new Cassandra4SchemaQueries(channel, node, config, logPrefix);
      } else {
        // 6.8+ uses DSE 6.8 schema (C* 4.0 schema with graph metadata) (JAVA-1898)
        return new Dse68SchemaQueries(channel, node, config, logPrefix);
      }
    } else {
      Version cassandraVersion = node.getCassandraVersion();
      if (cassandraVersion == null) {
        LOG.warn(
            "[{}] Cassandra version missing for {}, defaulting to {}",
            logPrefix,
            node,
            Version.V3_0_0);
        cassandraVersion = Version.V3_0_0;
      } else {
        cassandraVersion = cassandraVersion.nextStable();
      }
      LOG.debug(
          "[{}] Sending schema queries to {} with version {}", logPrefix, node, cassandraVersion);
      if (cassandraVersion.compareTo(Version.V2_2_0) < 0) {
        return new Cassandra21SchemaQueries(channel, node, config, logPrefix);
      } else if (cassandraVersion.compareTo(Version.V3_0_0) < 0) {
        return new Cassandra22SchemaQueries(channel, node, config, logPrefix);
      } else if (cassandraVersion.compareTo(Version.V4_0_0) < 0) {
        return new Cassandra3SchemaQueries(channel, node, config, logPrefix);
      } else {
        return new Cassandra4SchemaQueries(channel, node, config, logPrefix);
      }
    }
  }
}
