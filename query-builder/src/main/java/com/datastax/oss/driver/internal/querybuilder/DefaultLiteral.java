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
package com.datastax.oss.driver.internal.querybuilder;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultLiteral<ValueT> implements Literal {

  private final ValueT value;
  private final TypeCodec<ValueT> codec;
  private final CqlIdentifier alias;

  public DefaultLiteral(@Nullable ValueT value, @Nullable TypeCodec<ValueT> codec) {
    this(value, codec, null);
  }

  public DefaultLiteral(
      @Nullable ValueT value, @Nullable TypeCodec<ValueT> codec, @Nullable CqlIdentifier alias) {
    Preconditions.checkArgument(
        value == null || codec != null, "Must provide a codec if the value is not null");
    this.value = value;
    this.codec = codec;
    this.alias = alias;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    if (value == null) {
      builder.append("NULL");
    } else {
      builder.append(codec.format(value));
    }
    if (alias != null) {
      builder.append(" AS ").append(alias.asCql(true));
    }
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new DefaultLiteral<>(value, codec, alias);
  }

  @Nullable
  public ValueT getValue() {
    return value;
  }

  @Nullable
  public TypeCodec<ValueT> getCodec() {
    return codec;
  }

  @Nullable
  @Override
  public CqlIdentifier getAlias() {
    return alias;
  }
}
