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
package com.datastax.oss.driver.api.core.data;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * An immutable n-dimensional vector representation of the CQL vector type.
 *
 * <p>Instances may be created iteratively using {@link CqlVector.Builder} or in a single method
 * call via {@link #newInstance(Object[])} or {@link #newInstance(Collection)}.
 */
public class CqlVector<T> implements Iterable<T> {

  private final ImmutableList<T> values;

  private CqlVector(ImmutableList<T> values) {
    this.values = values;
  }

  /**
   * Creates a vector containing the specified elements
   *
   * @param values the values contained in the created vector
   */
  public static <T> CqlVector<T> newInstance(T... values) {
    return new CqlVector<T>(ImmutableList.copyOf(values));
  }

  /**
   * Creates a vector containing the specified elements
   *
   * @param values the values contained in the created vector
   */
  public static <T> CqlVector<T> newInstance(Collection<T> values) {
    return new CqlVector<T>(ImmutableList.copyOf(values));
  }

  /**
   * Create a new builder for building vectors
   *
   * @return the builder
   */
  public static <T> CqlVector.Builder<T> builder() {
    return new CqlVector.Builder<T>();
  }

  /**
   * Convert a <code>String</code> into a vector.
   *
   * <p>This method should understand the format used by {@link CqlVector#toString()}
   *
   * @param input the String to be converted
   * @param subtypeCodec an instance of <code>TypeCodec</code> to use when encoding extracted values
   */
  public static <T> CqlVector<T> from(@NonNull String input, TypeCodec<T> subtypeCodec) {

    ImmutableList<T> values =
        Streams.stream(Splitter.on(", ").split(input.substring(1, input.length() - 1)))
            .map(subtypeCodec::parse)
            .collect(ImmutableList.toImmutableList());
    return new CqlVector<T>(values);
  }

  @Override
  public Iterator<T> iterator() {
    return values.iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof CqlVector) {
      CqlVector that = (CqlVector) o;
      return this.values.equals(that.values);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values.toArray());
  }

  @Override
  public String toString() {
    return values == null ? "NULL" : Iterables.toString(values);
  }

  public static class Builder<T> {

    private ImmutableList.Builder<T> listBuilder;

    private Builder() {
      listBuilder = new ImmutableList.Builder<T>();
    }

    /**
     * Add the specified element to the builder
     *
     * @param element the element to add
     * @return this {@link Builder}
     */
    public Builder<T> add(T element) {
      listBuilder.add(element);
      return this;
    }

    /**
     * Add the specified elements to the builder
     *
     * @param elements the element to add
     * @return this {@link Builder}
     */
    public Builder<T> add(T... elements) {
      listBuilder.addAll(Iterators.forArray(elements));
      return this;
    }

    /**
     * Add the elements contained in the specified {@link Iterable} to the builder
     *
     * @param iter the {@link Iterable} containing the elements to add
     * @return this {@link Builder}
     */
    public Builder<T> addAll(Iterable<T> iter) {
      listBuilder.addAll(iter);
      return this;
    }

    public CqlVector<T> build() {
      return new CqlVector<T>(listBuilder.build());
    }
  }
}
