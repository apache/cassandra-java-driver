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
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class FieldSelector implements Selector {

  private final Selector udt;
  private final CqlIdentifier fieldId;
  private final CqlIdentifier alias;

  public FieldSelector(@NonNull Selector udt, @NonNull CqlIdentifier fieldId) {
    this(udt, fieldId, null);
  }

  public FieldSelector(
      @NonNull Selector udt, @NonNull CqlIdentifier fieldId, @Nullable CqlIdentifier alias) {
    Preconditions.checkNotNull(udt);
    Preconditions.checkNotNull(fieldId);
    this.udt = udt;
    this.fieldId = fieldId;
    this.alias = alias;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new FieldSelector(udt, fieldId, alias);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    udt.appendTo(builder);
    builder.append('.').append(fieldId.asCql(true));
    if (alias != null) {
      builder.append(" AS ").append(alias.asCql(true));
    }
  }

  @NonNull
  public Selector getUdt() {
    return udt;
  }

  @NonNull
  public CqlIdentifier getFieldId() {
    return fieldId;
  }

  @Nullable
  @Override
  public CqlIdentifier getAlias() {
    return alias;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof FieldSelector) {
      FieldSelector that = (FieldSelector) other;
      return this.udt.equals(that.udt)
          && this.fieldId.equals(that.fieldId)
          && Objects.equals(this.alias, that.alias);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(udt, fieldId, alias);
  }
}
