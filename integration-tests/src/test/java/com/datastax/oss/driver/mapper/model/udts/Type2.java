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
package com.datastax.oss.driver.mapper.model.udts;

import com.datastax.oss.driver.api.mapper.annotations.Entity;

@Entity
public class Type2 {
  private int i;

  public Type2() {}

  public Type2(int i) {
    this.i = i;
  }

  public int getI() {
    return i;
  }

  public void setI(int i) {
    this.i = i;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Type2) {
      Type2 that = (Type2) other;
      return this.i == that.i;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return i;
  }
}
