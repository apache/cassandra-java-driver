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

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class StartupOptionsBuilder {

  public static final String DRIVER_NAME_KEY = "DRIVER_NAME";
  public static final String DRIVER_VERSION_KEY = "DRIVER_VERSION";

  protected final InternalDriverContext context;

  public StartupOptionsBuilder(InternalDriverContext context) {
    this.context = context;
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
    NullAllowingImmutableMap.Builder<String, String> builder = NullAllowingImmutableMap.builder(3);
    // add compression (if configured) and driver name and version
    String compressionAlgorithm = context.getCompressor().algorithm();
    if (compressionAlgorithm != null && !compressionAlgorithm.trim().isEmpty()) {
      builder.put(Startup.COMPRESSION_KEY, compressionAlgorithm.trim());
    }
    return builder
        .put(DRIVER_NAME_KEY, getDriverName())
        .put(DRIVER_VERSION_KEY, getDriverVersion())
        .build();
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
