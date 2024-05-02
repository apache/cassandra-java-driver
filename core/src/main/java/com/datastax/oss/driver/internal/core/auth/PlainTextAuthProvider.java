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
package com.datastax.oss.driver.internal.core.auth;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.auth.AuthUtils;
import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

/**
 * A simple authentication provider that supports SASL authentication using the PLAIN mechanism for
 * version 3 (or above) of the CQL native protocol.
 *
 * <p>To activate this provider, add an {@code advanced.auth-provider} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.auth-provider {
 *     class = com.datastax.driver.api.core.auth.PlainTextAuthProvider
 *     username = cassandra
 *     password = cassandra
 *
 *     // If connecting to DataStax Enterprise, this additional option allows proxy authentication
 *     // (login as another user or role)
 *     authorization-id = userOrRole
 *   }
 * }
 * </pre>
 *
 * The authentication provider cannot be changed at runtime; however, the credentials can be changed
 * at runtime: the new ones will be used for new connection attempts once the configuration gets
 * {@linkplain com.datastax.oss.driver.api.core.config.DriverConfigLoader#reload() reloaded}.
 *
 * <p>See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class PlainTextAuthProvider extends PlainTextAuthProviderBase {

  private final DriverExecutionProfile config;

  public PlainTextAuthProvider(DriverContext context) {
    super(context.getSessionName());
    this.config = context.getConfig().getDefaultProfile();
  }

  @NonNull
  @Override
  protected Credentials getCredentials(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    // It's not valid to use the PlainTextAuthProvider without a username or password, error out
    // early here
    AuthUtils.validateConfigPresent(
        config,
        PlainTextAuthProvider.class.getName(),
        endPoint,
        DefaultDriverOption.AUTH_PROVIDER_USER_NAME,
        DefaultDriverOption.AUTH_PROVIDER_PASSWORD);

    String authorizationId = config.getString(DseDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, "");
    assert authorizationId != null; // per the default above
    return new Credentials(
        config.getString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME).toCharArray(),
        config.getString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD).toCharArray(),
        authorizationId.toCharArray());
  }
}
