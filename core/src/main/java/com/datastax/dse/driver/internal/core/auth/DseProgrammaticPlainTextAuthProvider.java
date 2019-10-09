/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.auth;

import com.datastax.dse.driver.api.core.auth.DsePlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;

public class DseProgrammaticPlainTextAuthProvider extends DsePlainTextAuthProviderBase {
  private final String authenticationId;
  private final String password;
  private final String authorizationId;

  public DseProgrammaticPlainTextAuthProvider(
      String authenticationId, String password, String authorizationId) {
    super("");
    this.authenticationId = authenticationId;
    this.password = password;
    this.authorizationId = authorizationId;
  }

  @NonNull
  @Override
  protected Credentials getCredentials(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    return new Credentials(
        authenticationId.toCharArray(), password.toCharArray(), authorizationId.toCharArray());
  }
}
