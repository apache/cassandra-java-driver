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
package com.datastax.oss.driver.internal.core.context;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.UUID;
import net.jcip.annotations.Immutable;

@Immutable
public class StartupOptionsBuilder {

  public static final String DRIVER_NAME_KEY = "DRIVER_NAME";
  public static final String DRIVER_VERSION_KEY = "DRIVER_VERSION";
  public static final String APPLICATION_NAME_KEY = "APPLICATION_NAME";
  public static final String APPLICATION_VERSION_KEY = "APPLICATION_VERSION";
  public static final String CLIENT_ID_KEY = "CLIENT_ID";

  protected final InternalDriverContext context;
  private UUID clientId;
  private String applicationName;
  private String applicationVersion;

  public StartupOptionsBuilder(InternalDriverContext context) {
    this.context = context;
  }

  /**
   * Sets the client ID to be sent in the Startup message options.
   *
   * <p>If this method is not invoked, or the id passed in is null, a random {@link UUID} will be
   * generated and used by default.
   */
  public StartupOptionsBuilder withClientId(@Nullable UUID clientId) {
    this.clientId = clientId;
    return this;
  }

  /**
   * Sets the client application name to be sent in the Startup message options.
   *
   * <p>If this method is not invoked, or the name passed in is null, no application name option
   * will be sent in the startup message options.
   */
  public StartupOptionsBuilder withApplicationName(@Nullable String applicationName) {
    this.applicationName = applicationName;
    return this;
  }

  /**
   * Sets the client application version to be sent in the Startup message options.
   *
   * <p>If this method is not invoked, or the name passed in is null, no application version option
   * will be sent in the startup message options.
   */
  public StartupOptionsBuilder withApplicationVersion(@Nullable String applicationVersion) {
    this.applicationVersion = applicationVersion;
    return this;
  }

  /**
   * Builds a map of options to send in a Startup message.
   *
   * <p>The default set of options are built here and include {@link
   * com.datastax.oss.protocol.internal.request.Startup#COMPRESSION_KEY} (if the context passed in
   * has a compressor/algorithm set), and the driver's {@link #DRIVER_NAME_KEY} and {@link
   * #DRIVER_VERSION_KEY}. The {@link com.datastax.oss.protocol.internal.request.Startup}
   * constructor will add {@link
   * com.datastax.oss.protocol.internal.request.Startup#CQL_VERSION_KEY}.
   *
   * @return Map of Startup Options.
   */
  public Map<String, String> build() {
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();

    NullAllowingImmutableMap.Builder<String, String> builder = NullAllowingImmutableMap.builder(3);
    // add compression (if configured) and driver name and version
    String compressionAlgorithm = context.getCompressor().algorithm();
    if (compressionAlgorithm != null && !compressionAlgorithm.trim().isEmpty()) {
      builder.put(Startup.COMPRESSION_KEY, compressionAlgorithm.trim());
    }
    builder.put(DRIVER_NAME_KEY, getDriverName()).put(DRIVER_VERSION_KEY, getDriverVersion());

    // Add Insights entries, falling back to generation / config if no programmatic values provided:
    if (clientId == null) {
      clientId = Uuids.random();
    }
    builder.put(CLIENT_ID_KEY, clientId.toString());
    if (applicationName == null) {
      applicationName = config.getString(DseDriverOption.APPLICATION_NAME, null);
    }
    if (applicationName != null) {
      builder.put(APPLICATION_NAME_KEY, applicationName);
    }
    if (applicationVersion == null) {
      applicationVersion = config.getString(DseDriverOption.APPLICATION_VERSION, null);
    }
    if (applicationVersion != null) {
      builder.put(APPLICATION_VERSION_KEY, applicationVersion);
    }

    return builder.build();
  }

  /**
   * Returns this driver's name.
   *
   * <p>By default, this method will pull from the bundled Driver.properties file. Subclasses should
   * override this method if they need to report a different Driver name on Startup.
   */
  protected String getDriverName() {
    return Session.OSS_DRIVER_COORDINATES.getName();
  }

  /**
   * Returns this driver's version.
   *
   * <p>By default, this method will pull from the bundled Driver.properties file. Subclasses should
   * override this method if they need to report a different Driver version on Startup.
   */
  protected String getDriverVersion() {
    return Session.OSS_DRIVER_COORDINATES.getVersion().toString();
  }
}
