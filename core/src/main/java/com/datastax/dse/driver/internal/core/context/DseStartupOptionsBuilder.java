/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.context;

import static com.datastax.dse.driver.api.core.DseSession.DSE_DRIVER_COORDINATES;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.UUID;

public class DseStartupOptionsBuilder extends StartupOptionsBuilder {

  public static final String APPLICATION_NAME_KEY = "APPLICATION_NAME";
  public static final String APPLICATION_VERSION_KEY = "APPLICATION_VERSION";
  public static final String CLIENT_ID_KEY = "CLIENT_ID";

  private UUID clientId;
  private String applicationName;
  private String applicationVersion;

  public DseStartupOptionsBuilder(InternalDriverContext context) {
    super(context);
  }

  @Override
  protected String getDriverVersion() {
    // use the DSE Version instead
    return DSE_DRIVER_COORDINATES.getVersion().toString();
  }

  @Override
  protected String getDriverName() {
    return DSE_DRIVER_COORDINATES.getName();
  }

  /**
   * Sets the client ID to be sent in the Startup message options.
   *
   * <p>If this method is not invoked, or the id passed in is null, a random {@link UUID} will be
   * generated and used by default.
   */
  public DseStartupOptionsBuilder withClientId(@Nullable UUID clientId) {
    this.clientId = clientId;
    return this;
  }

  /**
   * Sets the client application name to be sent in the Startup message options.
   *
   * <p>If this method is not invoked, or the name passed in is null, no application name option
   * will be sent in the startup message options.
   */
  public DseStartupOptionsBuilder withApplicationName(@Nullable String applicationName) {
    this.applicationName = applicationName;
    return this;
  }

  /**
   * Sets the client application version to be sent in the Startup message options.
   *
   * <p>If this method is not invoked, or the name passed in is null, no application version option
   * will be sent in the startup message options.
   */
  public DseStartupOptionsBuilder withApplicationVersion(@Nullable String applicationVersion) {
    this.applicationVersion = applicationVersion;
    return this;
  }

  @Override
  public Map<String, String> build() {

    DriverExecutionProfile config = context.getConfig().getDefaultProfile();

    // Fall back to generation / config if no programmatic values provided:
    if (clientId == null) {
      clientId = Uuids.random();
    }
    if (applicationName == null) {
      applicationName = config.getString(DseDriverOption.APPLICATION_NAME, null);
    }
    if (applicationVersion == null) {
      applicationVersion = config.getString(DseDriverOption.APPLICATION_VERSION, null);
    }

    NullAllowingImmutableMap.Builder<String, String> builder =
        NullAllowingImmutableMap.<String, String>builder().putAll(super.build());

    builder.put(CLIENT_ID_KEY, clientId.toString());
    if (applicationName != null) {
      builder.put(APPLICATION_NAME_KEY, applicationName);
    }
    if (applicationVersion != null) {
      builder.put(APPLICATION_VERSION_KEY, applicationVersion);
    }

    return builder.build();
  }
}
