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
package com.datastax.oss.driver.internal.querybuilder.term;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class FunctionTerm implements Term {

  private final CqlIdentifier keyspaceId;
  private final CqlIdentifier functionId;
  private final Iterable<Term> arguments;

  public FunctionTerm(
      @Nullable CqlIdentifier keyspaceId,
      @NonNull CqlIdentifier functionId,
      @NonNull Iterable<Term> arguments) {
    Preconditions.checkNotNull(functionId);
    Preconditions.checkNotNull(arguments);
    this.keyspaceId = keyspaceId;
    this.functionId = functionId;
    this.arguments = arguments;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    // The function name appears even without arguments, so don't use prefix/suffix in CqlHelper
    CqlHelper.qualify(keyspaceId, functionId, builder);
    builder.append('(');
    CqlHelper.append(arguments, builder, null, ",", null);
    builder.append(')');
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @Nullable
  public CqlIdentifier getKeyspaceId() {
    return keyspaceId;
  }

  @NonNull
  public CqlIdentifier getFunctionId() {
    return functionId;
  }

  @NonNull
  public Iterable<Term> getArguments() {
    return arguments;
  }
}
