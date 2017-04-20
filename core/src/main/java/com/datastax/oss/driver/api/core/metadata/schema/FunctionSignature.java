/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

/**
 * The signature that uniquely identifies a CQL function or aggregate in a keyspace.
 *
 * <p>It's composed of a name and a list of parameter types. Overloads (such as {@code sum(int)} and
 * {@code sum(int, int)} are not equal.
 */
public class FunctionSignature {
  private final CqlIdentifier name;
  private final List<DataType> parameterTypes;

  public FunctionSignature(CqlIdentifier name, Iterable<DataType> parameterTypes) {
    this.name = name;
    this.parameterTypes = ImmutableList.copyOf(parameterTypes);
  }

  public FunctionSignature(CqlIdentifier name, DataType... parameterTypes) {
    this(name, ImmutableList.<DataType>builder().add(parameterTypes).build());
  }

  public CqlIdentifier getName() {
    return name;
  }

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
}
