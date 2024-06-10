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
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class RangeSelector implements Selector {

  private final Selector collection;
  private final Term left;
  private final Term right;
  private final CqlIdentifier alias;

  public RangeSelector(@NonNull Selector collection, @Nullable Term left, @Nullable Term right) {
    this(collection, left, right, null);
  }

  public RangeSelector(
      @NonNull Selector collection,
      @Nullable Term left,
      @Nullable Term right,
      @Nullable CqlIdentifier alias) {
    Preconditions.checkNotNull(collection);
    Preconditions.checkArgument(
        left != null || right != null, "At least one of the bounds must be specified");
    this.collection = collection;
    this.left = left;
    this.right = right;
    this.alias = alias;
  }

  @NonNull
  @Override
  public Selector as(@NonNull CqlIdentifier alias) {
    return new RangeSelector(collection, left, right, alias);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    collection.appendTo(builder);
    builder.append('[');
    if (left != null) {
      left.appendTo(builder);
    }
    builder.append("..");
    if (right != null) {
      right.appendTo(builder);
    }
    builder.append(']');
    if (alias != null) {
      builder.append(" AS ").append(alias.asCql(true));
    }
  }

  @NonNull
  public Selector getCollection() {
    return collection;
  }

  @Nullable
  public Term getLeft() {
    return left;
  }

  @Nullable
  public Term getRight() {
    return right;
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
    } else if (other instanceof RangeSelector) {
      RangeSelector that = (RangeSelector) other;
      return this.collection.equals(that.collection)
          && Objects.equals(this.left, that.left)
          && Objects.equals(this.right, that.right)
          && Objects.equals(this.alias, that.alias);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(collection, left, right, alias);
  }
}
