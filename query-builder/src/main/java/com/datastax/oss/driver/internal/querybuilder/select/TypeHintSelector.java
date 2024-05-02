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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class TypeHintSelector implements Selector {

  private final Selector selector;
  private final DataType targetType;
  private final CqlIdentifier alias;

  public TypeHintSelector(@NonNull Selector selector, @NonNull DataType targetType) {
    this(selector, targetType, null);
  }

  public TypeHintSelector(
      @NonNull Selector selector, @NonNull DataType targetType, @Nullable CqlIdentifier alias) {
    Preconditions.checkNotNull(selector);
    Preconditions.checkNotNull(targetType);
    this.selector = selector;
    this.targetType = targetType;
    this.alias = alias;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new TypeHintSelector(selector, targetType, alias);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append('(').append(targetType.asCql(false, true)).append(')');
    selector.appendTo(builder);
    if (alias != null) {
      builder.append(" AS ").append(alias.asCql(true));
    }
  }

  @NonNull
  public Selector getSelector() {
    return selector;
  }

  @NonNull
  public DataType getTargetType() {
    return targetType;
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
    } else if (other instanceof TypeHintSelector) {
      TypeHintSelector that = (TypeHintSelector) other;
      return this.selector.equals(that.selector)
          && this.targetType.equals(that.targetType)
          && Objects.equals(this.alias, that.alias);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(selector, targetType, alias);
  }
}
