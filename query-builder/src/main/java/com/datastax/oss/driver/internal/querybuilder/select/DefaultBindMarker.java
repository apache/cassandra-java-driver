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
package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultBindMarker implements BindMarker {

  private final CqlIdentifier id;

  public DefaultBindMarker(@Nullable CqlIdentifier id) {
    this.id = id;
  }

  public DefaultBindMarker() {
    this(null);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    if (id == null) {
      builder.append('?');
    } else {
      builder.append(':').append(id.asCql(true));
    }
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Nullable
  public CqlIdentifier getId() {
    return id;
  }
}
