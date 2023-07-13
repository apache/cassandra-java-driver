/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.mapper;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Factors common code for mapper tests that rely on a simple inventory model. */
public abstract class InventoryITBase {

  protected static Product FLAMETHROWER =
      new Product(UUID.randomUUID(), "Flamethrower", new InventoryITBase.Dimensions(30, 10, 8));
  protected static Product MP3_DOWNLOAD = new Product(UUID.randomUUID(), "MP3 download", null);

  protected static List<String> createStatements(CcmRule ccmRule) {
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builder()
            .add(
                "CREATE TYPE dimensions(length int, width int, height int)",
                "CREATE TABLE product(id uuid PRIMARY KEY, description text, dimensions frozen<dimensions>)",
                "CREATE TABLE product_without_id(id uuid, clustering int, description text, "
                    + "PRIMARY KEY((id), clustering))");

    if (supportsSASI(ccmRule)) {
      builder.add(
          "CREATE CUSTOM INDEX product_description ON product(description) "
              + "USING 'org.apache.cassandra.index.sasi.SASIIndex' "
              + "WITH OPTIONS = {"
              + "'mode': 'CONTAINS',"
              + "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',"
              + "'tokenization_enable_stemming': 'true',"
              + "'tokenization_locale': 'en',"
              + "'tokenization_skip_stop_words': 'true',"
              + "'analyzed': 'true',"
              + "'tokenization_normalize_lowercase': 'true'"
              + "}");
    }

    return builder.build();
  }

  private static final Version MINIMUM_SASI_VERSION = Version.parse("3.4.0");

  protected static boolean supportsSASI(CcmRule ccmRule) {
    return ccmRule.getCassandraVersion().compareTo(MINIMUM_SASI_VERSION) >= 0;
  }

  @Entity
  public static class Product {

    @PartitionKey private UUID id;
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

  @Entity
  public static class ProductWithoutId {
    private String description;

    public ProductWithoutId() {}

    public ProductWithoutId(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductWithoutId that = (ProductWithoutId) o;
      return Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
      return Objects.hash(description);
    }

    @Override
    public String toString() {
      return "ProductWithoutId{" + "description='" + description + '\'' + '}';
    }
  }

  @Entity
  public static class Dimensions {

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

  @Entity
  public static class OnlyPK {
    @PartitionKey private UUID id;

    public OnlyPK() {}

    public OnlyPK(UUID id) {
      this.id = id;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      OnlyPK onlyPK = (OnlyPK) o;
      return Objects.equals(id, onlyPK.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }

    @Override
    public String toString() {
      return "OnlyPK{" + "id=" + id + '}';
    }
  }
}
