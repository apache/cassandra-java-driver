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
package com.datastax.oss.driver.internal.querybuilder.lhs;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class TokenLeftOperand implements LeftOperand {

  private final Iterable<CqlIdentifier> identifiers;

  public TokenLeftOperand(@NonNull Iterable<CqlIdentifier> identifiers) {
    this.identifiers = identifiers;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    CqlHelper.appendIds(identifiers, builder, "token(", ",", ")");
  }

  public @NonNull Iterable<CqlIdentifier> getIdentifiers() {
    return identifiers;
  }
}
