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
import java.util.UUID;

@Entity
public class Product {

  private UUID id;
  private String description;
  private Dimensions dimensions;

  public Product() {}

  public Product(UUID id, String description, Dimensions dimensions) {
    this.id = id;
    this.description = description;
    this.dimensions = dimensions;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Dimensions getDimensions() {
    return dimensions;
  }

  public void setDimensions(Dimensions dimensions) {
    this.dimensions = dimensions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Product product = (Product) o;
    return Objects.equals(id, product.id)
        && Objects.equals(description, product.description)
        && Objects.equals(dimensions, product.dimensions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, description, dimensions);
  }

  @Override
  public String toString() {
    return "Product{"
        + "id="
        + id
        + ", description='"
        + description
        + '\''
        + ", dimensions="
        + dimensions
        + '}';
  }
}
