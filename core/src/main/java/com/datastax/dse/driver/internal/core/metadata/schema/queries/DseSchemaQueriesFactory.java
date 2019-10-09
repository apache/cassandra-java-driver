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
package com.datastax.dse.driver.internal.core.metadata.schema.queries;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.Cassandra21SchemaQueries;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.Cassandra3SchemaQueries;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.Cassandra4SchemaQueries;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.DefaultSchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueries;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletableFuture;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DseSchemaQueriesFactory extends DefaultSchemaQueriesFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaQueriesFactory.class);

  @NonNull private static final Version V5_0_0 = Version.parse("5.0.0");
  @NonNull private static final Version V6_7_0 = Version.parse("6.7.0");

  public DseSchemaQueriesFactory(InternalDriverContext context) {
    super(context);
  }

  @Override
  protected SchemaQueries newInstance(
      Node node, DriverChannel channel, CompletableFuture<Metadata> refreshFuture) {
    Object versionObj = node.getExtras().get(DseNodeProperties.DSE_VERSION);
    Version version;
    if (versionObj == null) {
      LOG.warn("[{}] DSE version missing for {}, deferring to C* version", logPrefix, node);
      return super.newInstance(node, channel, refreshFuture);
    }

    version = ((Version) versionObj).nextStable();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    LOG.debug("[{}] Sending schema queries to {} with DSE version {}", logPrefix, node, version);
    // 4.8 is the oldest version supported, which uses C* 2.1 schema
    if (version.compareTo(V5_0_0) < 0) {
      return new Cassandra21SchemaQueries(channel, refreshFuture, config, logPrefix);
    } else if (version.compareTo(V6_7_0) < 0) {
      // 5.0 - 6.7 uses C* 3.0 schema
      return new Cassandra3SchemaQueries(channel, refreshFuture, config, logPrefix);
    } else {
      // 6.7+ uses C* 4.0 schema
      return new Cassandra4SchemaQueries(channel, refreshFuture, config, logPrefix);
    }
  }
}
