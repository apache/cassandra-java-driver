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
package com.datastax.dse.driver.api.core.auth;

import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

/**
 * @deprecated The driver's default plain text providers now support both Apache Cassandra and DSE.
 *     This type was preserved for backward compatibility, but implementors should now extend {@link
 *     PlainTextAuthProviderBase} instead.
 */
@ThreadSafe
@Deprecated
public abstract class DsePlainTextAuthProviderBase extends PlainTextAuthProviderBase {

  protected DsePlainTextAuthProviderBase(@NonNull String logPrefix) {
    super(logPrefix);
  }
}
