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
package com.datastax.dse.driver.api.core;

import com.datastax.dse.driver.internal.core.session.DefaultDseSession;
import com.datastax.oss.driver.api.core.CqlSession;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.NotThreadSafe;

/**
 * Helper class to build a {@link DseSession} instance.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class DseSessionBuilder extends DseSessionBuilderBase<DseSessionBuilder, DseSession> {

  @NonNull
  @Override
  protected DseSession wrap(@NonNull CqlSession defaultSession) {
    return new DefaultDseSession(defaultSession);
  }
}
