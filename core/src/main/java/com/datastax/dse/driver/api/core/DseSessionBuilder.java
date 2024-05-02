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
package com.datastax.dse.driver.api.core;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.NotThreadSafe;

/**
 * @deprecated DSE functionality is now exposed directly on {@link CqlSession}. This class is
 *     preserved for backward compatibility, but {@link CqlSession#builder()} should be used
 *     instead.
 */
@NotThreadSafe
@Deprecated
public class DseSessionBuilder extends SessionBuilder<DseSessionBuilder, DseSession> {

  @NonNull
  @Override
  protected DseSession wrap(@NonNull CqlSession defaultSession) {
    return new com.datastax.dse.driver.internal.core.session.DefaultDseSession(defaultSession);
  }
}
