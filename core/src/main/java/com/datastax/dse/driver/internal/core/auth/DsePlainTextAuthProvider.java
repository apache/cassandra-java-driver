/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.auth;

import com.datastax.dse.driver.api.core.auth.DsePlainTextAuthProviderBase;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

/**
 * An authentication provider that supports SASL authentication using the PLAIN mechanism to connect
 * to DSE clusters secured with DseAuthenticator.
 *
 * <p>To activate this provider, an {@code auth-provider} section must be included in the driver
 * configuration, for example:
 *
 * <pre>
 * dse-java-driver {
 *   auth-provider {
 *     class = com.datastax.dse.driver.internal.core.auth.DsePlainTextAuthProvider
 *     username = user0
 *     password = mypassword
 *     authorization-id = user1
 *   }
 * }
 * </pre>
 *
 * See the {@code dse-reference.conf} file included with the driver for more information.
 */
@ThreadSafe
public class DsePlainTextAuthProvider extends DsePlainTextAuthProviderBase {

  private final DriverExecutionProfile config;

  public DsePlainTextAuthProvider(DriverContext context) {
    super(context.getSessionName());
    this.config = context.getConfig().getDefaultProfile();
  }

  @NonNull
  @Override
  protected Credentials getCredentials(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    String authorizationId;
    if (config.isDefined(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID)) {
      authorizationId = config.getString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID);
    } else {
      authorizationId = "";
    }
    // It's not valid to use the DsePlainTextAuthProvider without a username or password, error out
    // early here
    AuthUtils.validateConfigPresent(
        config,
        DsePlainTextAuthProvider.class.getName(),
        endPoint,
        DefaultDriverOption.AUTH_PROVIDER_USER_NAME,
        DefaultDriverOption.AUTH_PROVIDER_PASSWORD);
    return new Credentials(
        config.getString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME).toCharArray(),
        config.getString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD).toCharArray(),
        authorizationId.toCharArray());
  }
}
