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
package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class FunctionSelector extends CollectionSelector {

  private final CqlIdentifier keyspaceId;
  private final CqlIdentifier functionId;

  public FunctionSelector(
      @Nullable CqlIdentifier keyspaceId,
      @NonNull CqlIdentifier functionId,
      @NonNull Iterable<Selector> arguments) {
    this(keyspaceId, functionId, arguments, null);
  }

  public FunctionSelector(
      @Nullable CqlIdentifier keyspaceId,
      @NonNull CqlIdentifier functionId,
      @NonNull Iterable<Selector> elementSelectors,
      @Nullable CqlIdentifier alias) {
    super(elementSelectors, buildOpening(keyspaceId, functionId), ")", alias);
    this.keyspaceId = keyspaceId;
    this.functionId = functionId;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new FunctionSelector(keyspaceId, functionId, getElementSelectors(), alias);
  }

  @Nullable
  public CqlIdentifier getKeyspaceId() {
    return keyspaceId;
  }

  @NonNull
  public CqlIdentifier getFunctionId() {
    return functionId;
  }

  /** Returns the arguments of the function. */
  @NonNull
  @Override
  public Iterable<Selector> getElementSelectors() {
    // Overridden only to customize the javadoc
    return super.getElementSelectors();
  }

  private static String buildOpening(CqlIdentifier keyspaceId, CqlIdentifier functionId) {
    return (keyspaceId == null)
        ? functionId.asCql(true) + "("
        : keyspaceId.asCql(true) + "." + functionId.asCql(true) + "(";
  }
}
