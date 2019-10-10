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
package com.datastax.dse.driver.internal.core.context;

import com.datastax.dse.driver.api.core.session.DseProgrammaticArguments;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;

/**
 * @deprecated All DSE functionality is now available directly on {@link CqlSession}. This type is
 *     preserved for backward compatibility, but clients should now build {@link CqlSession}
 *     instances instead of DSE sessions.
 */
@ThreadSafe
@Deprecated
public class DseDriverContext extends DefaultDriverContext {

  public DseDriverContext(
      DriverConfigLoader configLoader,
      ProgrammaticArguments programmaticArguments,
      DseProgrammaticArguments dseProgrammaticArguments) {
    super(configLoader, programmaticArguments);
  }
  /**
   * @deprecated this constructor only exists for backward compatibility. Please use {@link
   *     #DseDriverContext(DriverConfigLoader, ProgrammaticArguments, DseProgrammaticArguments)}
   *     instead.
   */
  public DseDriverContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, String> localDatacenters,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader,
      UUID clientId,
      String applicationName,
      String applicationVersion) {
    this(
        configLoader,
        ProgrammaticArguments.builder()
            .addTypeCodecs(typeCodecs.toArray(new TypeCodec<?>[0]))
            .withNodeStateListener(nodeStateListener)
            .withSchemaChangeListener(schemaChangeListener)
            .withRequestTracker(requestTracker)
            .withLocalDatacenters(localDatacenters)
            .withNodeFilters(nodeFilters)
            .withClassLoader(classLoader)
            .build(),
        DseProgrammaticArguments.builder()
            .withStartupClientId(clientId)
            .withStartupApplicationName(applicationName)
            .withStartupApplicationVersion(applicationVersion)
            .build());
  }
}
