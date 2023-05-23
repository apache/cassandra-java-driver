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

import java.util.Arrays;

/** An n-dimensional vector defined in CQL */
public class CqlVector {

  private final float[] dimensions;

  public CqlVector(float... dimensions) {
    this.dimensions = dimensions;
  }

  public float[] getDimensions() {
    return Arrays.copyOf(this.dimensions, this.dimensions.length);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o instanceof CqlVector) {
      CqlVector that = (CqlVector) o;
      return Arrays.equals(that.dimensions, this.dimensions);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(dimensions);
  }

  @Override
  public String toString() {
    return "CqlVector{" + Arrays.toString(dimensions) + '}';
  }
}
