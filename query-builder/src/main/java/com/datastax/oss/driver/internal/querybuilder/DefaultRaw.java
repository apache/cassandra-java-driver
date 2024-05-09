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
package com.datastax.oss.driver.internal.querybuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.Raw;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultRaw implements Raw {

  private final String rawExpression;
  private final CqlIdentifier alias;

  public DefaultRaw(@NonNull String rawExpression) {
    this(rawExpression, null);
  }

  private DefaultRaw(@NonNull String rawExpression, @Nullable CqlIdentifier alias) {
    Preconditions.checkNotNull(rawExpression);
    this.rawExpression = rawExpression;
    this.alias = alias;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new DefaultRaw(rawExpression, alias);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(rawExpression);
    if (alias != null) {
      builder.append(" AS ").append(alias.asCql(true));
    }
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @NonNull
  public String getRawExpression() {
    return rawExpression;
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
    } else if (other instanceof DefaultRaw) {
      DefaultRaw that = (DefaultRaw) other;
      return this.rawExpression.equals(that.rawExpression)
          && Objects.equals(this.alias, that.alias);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(rawExpression, alias);
  }
}
