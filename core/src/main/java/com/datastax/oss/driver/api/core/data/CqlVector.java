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

import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import java.util.Arrays;

/** An n-dimensional vector defined in CQL */
public class CqlVector<T> {

  private final ImmutableList<T> values;

  private CqlVector(ImmutableList<T> values) {
    this.values = values;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Iterable<T> getValues() {
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

    String contents = Joiner.on(", ").join(this.values);
    return "CqlVector{" + contents + '}';
  }

  public static class Builder<T> {

    private ImmutableList.Builder<T> listBuilder;

    private Builder() {
      listBuilder = new ImmutableList.Builder<T>();
    }

    public Builder add(T element) {
      listBuilder.add(element);
      return this;
    }

    public Builder add(T... elements) {
      listBuilder.addAll(Iterators.forArray(elements));
      return this;
    }

    public Builder addAll(Iterable<T> iter) {
      listBuilder.addAll(iter);
      return this;
    }

    public CqlVector<T> build() {
      return new CqlVector<T>(listBuilder.build());
    }
  }
}
