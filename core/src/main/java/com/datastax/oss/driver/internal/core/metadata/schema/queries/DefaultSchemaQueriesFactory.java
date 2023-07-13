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

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.NodeProperties;
import java.util.concurrent.CompletableFuture;
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
  public SchemaQueries newInstance(CompletableFuture<Metadata> refreshFuture) {
    DriverChannel channel = context.getControlConnection().channel();
    if (channel == null || channel.closeFuture().isDone()) {
      throw new IllegalStateException("Control channel not available, aborting schema refresh");
    }
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
    return newInstance(node, channel, refreshFuture);
  }

  protected SchemaQueries newInstance(
      Node node, DriverChannel channel, CompletableFuture<Metadata> refreshFuture) {
    Version version = node.getCassandraVersion();
    if (version == null) {
      LOG.warn(
          "[{}] Cassandra version missing for {}, defaulting to {}",
          logPrefix,
          node,
          Version.V3_0_0);
      version = Version.V3_0_0;
    } else {
      version = version.nextStable();
    }
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    LOG.debug("[{}] Sending schema queries to {} with version {}", logPrefix, node, version);
    if (version.compareTo(Version.V2_2_0) < 0) {
      return new Cassandra21SchemaQueries(channel, refreshFuture, config, logPrefix);
    } else if (version.compareTo(Version.V3_0_0) < 0) {
      return new Cassandra22SchemaQueries(channel, refreshFuture, config, logPrefix);
    } else if (version.compareTo(Version.V4_0_0) < 0) {
      return new Cassandra3SchemaQueries(channel, refreshFuture, config, logPrefix);
    } else {

      // A bit of custom logic for DSE 6.0.x.  These versions report a Cassandra version of 4.0.0
      // but don't have support for system_virtual_schema tables supported by that version.  To
      // compensate we return the Cassandra 3 schema queries here for those versions
      if (node.getExtras().containsKey(NodeProperties.DSE_VERSION)) {

        Object dseVersionObj = node.getExtras().get(NodeProperties.DSE_VERSION);
        assert (dseVersionObj instanceof Version);
        if (((Version) dseVersionObj).compareTo(Version.V6_7_0) < 0) {

          return new Cassandra3SchemaQueries(channel, refreshFuture, config, logPrefix);
        }
      }
      return new Cassandra4SchemaQueries(channel, refreshFuture, config, logPrefix);
    }
  }
}
