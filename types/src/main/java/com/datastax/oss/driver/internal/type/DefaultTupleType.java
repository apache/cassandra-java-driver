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
package com.datastax.oss.driver.internal.type;

import com.datastax.oss.driver.api.type.DataType;
import com.datastax.oss.driver.api.type.TupleType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.List;

public class DefaultTupleType implements TupleType {

  private final List<DataType> componentTypes;

  public DefaultTupleType(List<DataType> componentTypes) {
    Preconditions.checkNotNull(componentTypes);
    this.componentTypes = componentTypes;
  }

  @Override
  public List<DataType> getComponentTypes() {
    return componentTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TupleType) {
      TupleType that = (TupleType) other;
      return this.componentTypes.equals(that.getComponentTypes());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return componentTypes.hashCode();
  }

  @Override
  public String toString() {
    return "Tuple(" + WITH_COMMA.join(componentTypes) + ")";
  }

  private static final Joiner WITH_COMMA = Joiner.on(", ");
}
