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
import java.util.Objects;

@Entity
public class Type1 {
  private String s;

  public Type1() {}

  public Type1(String s) {
    this.s = s;
  }

  public String getS() {
    return s;
  }

  public void setS(String s) {
    this.s = s;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Type1) {
      Type1 that = (Type1) other;
      return Objects.equals(this.s, that.s);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return s == null ? 0 : s.hashCode();
  }
}
