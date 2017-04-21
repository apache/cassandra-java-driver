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
package com.datastax.oss.driver.internal.types;

import com.datastax.oss.driver.api.types.CustomType;
import com.google.common.base.Preconditions;

public class DefaultCustomType implements CustomType {
  private final String className;

  public DefaultCustomType(String className) {
    Preconditions.checkNotNull(className);
    this.className = className;
  }

  @Override
  public String getClassName() {
    return className;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof CustomType) {
      CustomType that = (CustomType) other;
      return this.className.equals(that.getClassName());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return className.hashCode();
  }

  @Override
  public String toString() {
    return "Custom(" + className + ")";
  }
}
