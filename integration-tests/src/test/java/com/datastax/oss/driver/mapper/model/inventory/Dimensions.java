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
package com.datastax.oss.driver.mapper.model.inventory;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import java.util.Objects;

@Entity
public class Dimensions {

  private int length;
  private int width;
  private int height;

  public Dimensions() {}

  public Dimensions(int length, int width, int height) {
    this.length = length;
    this.width = width;
    this.height = height;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    this.width = width;
  }

  public int getHeight() {
    return height;
  }

  public void setHeight(int height) {
    this.height = height;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dimensions that = (Dimensions) o;
    return length == that.length && width == that.width && height == that.height;
  }

  @Override
  public int hashCode() {
    return Objects.hash(length, width, height);
  }

  @Override
  public String toString() {
    return "Dimensions{" + "length=" + length + ", width=" + width + ", height=" + height + '}';
  }
}
