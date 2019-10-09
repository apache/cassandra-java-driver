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
