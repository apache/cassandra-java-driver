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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * The signature that uniquely identifies a CQL function or aggregate in a keyspace.
 *
 * <p>It's composed of a name and a list of parameter types. Overloads (such as {@code sum(int)} and
 * {@code sum(int, int)} are not equal.
 */
@Immutable
public class FunctionSignature implements Serializable {

  private static final long serialVersionUID = 1;

  @NonNull private final CqlIdentifier name;
  @NonNull private final List<DataType> parameterTypes;

  public FunctionSignature(
      @NonNull CqlIdentifier name, @NonNull Iterable<DataType> parameterTypes) {
    this.name = name;
    this.parameterTypes = ImmutableList.copyOf(parameterTypes);
  }

  /**
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  public FunctionSignature(@NonNull CqlIdentifier name, @NonNull DataType... parameterTypes) {
    this(
        name,
        parameterTypes.length == 0
            ? ImmutableList.of()
            : ImmutableList.<DataType>builder().add(parameterTypes).build());
  }

  /**
   * Shortcut for {@link #FunctionSignature(CqlIdentifier, Iterable) new
   * FunctionSignature(CqlIdentifier.fromCql(name), parameterTypes)}.
   */
  public FunctionSignature(@NonNull String name, @NonNull Iterable<DataType> parameterTypes) {
    this(CqlIdentifier.fromCql(name), parameterTypes);
  }

  /**
   * Shortcut for {@link #FunctionSignature(CqlIdentifier, DataType...)} new
   * FunctionSignature(CqlIdentifier.fromCql(name), parameterTypes)}.
   *
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  public FunctionSignature(@NonNull String name, @NonNull DataType... parameterTypes) {
    this(CqlIdentifier.fromCql(name), parameterTypes);
  }

  @NonNull
  public CqlIdentifier getName() {
    return name;
  }

  @NonNull
  public List<DataType> getParameterTypes() {
    return parameterTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof FunctionSignature) {
      FunctionSignature that = (FunctionSignature) other;
      return this.name.equals(that.name) && this.parameterTypes.equals(that.parameterTypes);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, parameterTypes);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(name.asInternal()).append('(');
    boolean first = true;
    for (DataType type : parameterTypes) {
      if (first) {
        first = false;
      } else {
        builder.append(", ");
      }
      builder.append(type.asCql(true, true));
    }
    return builder.append(')').toString();
  }
}
