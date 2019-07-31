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
package com.datastax.oss.driver.internal.core.auth;

import com.datastax.oss.driver.api.core.session.SessionBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

/**
 * Alternative plaintext auth provider that receives the credentials programmatically instead of
 * pulling them from the configuration.
 *
 * @see SessionBuilder#withAuthCredentials(String, String)
 */
@ThreadSafe
public class ProgrammaticPlainTextAuthProvider extends PlainTextAuthProviderBase {
  private final String username;
  private final String password;

  public ProgrammaticPlainTextAuthProvider(String username, String password) {
    // This will typically be built before the session so we don't know the log prefix yet. Pass an
    // empty string, it's only used in one log message.
    super("");
    this.username = username;
    this.password = password;
  }

  @NonNull
  @Override
  protected Credentials getCredentials() {
    return new Credentials(username.toCharArray(), password.toCharArray());
  }
}
