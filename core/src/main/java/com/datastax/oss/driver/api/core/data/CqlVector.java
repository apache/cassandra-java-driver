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

import java.util.*;

/**
 * An n-dimensional vector defined in CQL. You can use either {@link CqlVector#builder()} for an
 * iterable interface or {@link #CqlVector(List)} directly. Once created, a CqlVector is immutable.
 */
public class CqlVector<T> implements Iterable<T> {

  private final List<T> values;

  /**
   * Create a CqlVector from a list of values.
   *
   * @param values
   */
  public CqlVector(List<T> values) {
    this.values = Collections.unmodifiableList(values);
  }

  public static <T> CqlVector of(T... values) {
    return new CqlVector(Collections.unmodifiableList(Arrays.asList(values)));
  }

  /** @return the (immutable) list of values in the vector */
  public List<T> getValues() {
    return values;
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
    StringBuilder builder = new StringBuilder("CqlVector{");
    for (T value : values) {
      builder.append(value).append(", ");
    }
    builder.setLength(builder.length() - ", ".length());
    builder.append("}");
    return builder.toString();
  }

  @Override
  public Iterator<T> iterator() {
    return values.iterator();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder<T> {
    private List<T> vector;

    private Builder() {
      this.vector = new ArrayList<>();
    }

    public Builder add(T element) {
      vector.add(element);
      return this;
    }

    public Builder add(T... elements) {
      vector.addAll(Arrays.asList(elements));
      return this;
    }

    public Builder addAll(Iterable<T> iter) {
      iter.forEach(vector::add);
      return this;
    }

    public CqlVector<T> build() {
      return new CqlVector<>(vector);
    }
  }
}
