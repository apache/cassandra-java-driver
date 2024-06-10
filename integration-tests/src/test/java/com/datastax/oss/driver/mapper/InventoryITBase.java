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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.mapper;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Factors common code for mapper tests that rely on a simple inventory model. */
public abstract class InventoryITBase {

  protected static Product FLAMETHROWER =
      new Product(UUID.randomUUID(), "Flamethrower", new InventoryITBase.Dimensions(30, 10, 8));
  protected static Product MP3_DOWNLOAD = new Product(UUID.randomUUID(), "MP3 download", null);

  protected static String DATE_1 = "2019-06-27";
  protected static String DATE_2 = "2019-06-28";
  protected static String DATE_3 = "2019-01-01";

  protected static ProductSale FLAMETHROWER_SALE_1 =
      new ProductSale(FLAMETHROWER.getId(), DATE_1, 1, Uuids.startOf(1561643130), 500.00, 5);

  protected static ProductSale FLAMETHROWER_SALE_2 =
      new ProductSale(FLAMETHROWER.getId(), DATE_1, 2, Uuids.startOf(1561645130), 500.00, 1);

  protected static ProductSale FLAMETHROWER_SALE_3 =
      new ProductSale(FLAMETHROWER.getId(), DATE_1, 1, Uuids.startOf(1561653130), 500.00, 2);

  protected static ProductSale FLAMETHROWER_SALE_4 =
      new ProductSale(FLAMETHROWER.getId(), DATE_1, 1, Uuids.startOf(1561657504), 702.00, 3);

  protected static ProductSale FLAMETHROWER_SALE_5 =
      new ProductSale(FLAMETHROWER.getId(), DATE_2, 1, Uuids.startOf(1561729530), 500.00, 23);

  protected static ProductSale MP3_DOWNLOAD_SALE_1 =
      new ProductSale(MP3_DOWNLOAD.getId(), DATE_3, 7, Uuids.startOf(915192000), 0.99, 12);

  protected static List<String> createStatements(CcmRule ccmRule) {
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builder()
            .add(
                "CREATE TYPE dimensions(length int, width int, height int)",
                "CREATE TABLE product(id uuid PRIMARY KEY, description text, dimensions frozen<dimensions>)",
                "CREATE TYPE dimensions2d(width int, height int)",
                "CREATE TABLE product2d(id uuid PRIMARY KEY, description text, dimensions frozen<dimensions2d>)",
                "CREATE TABLE product_without_id(id uuid, clustering int, description text, "
                    + "PRIMARY KEY((id), clustering))",
                "CREATE TABLE product_sale(id uuid, day text, ts uuid, customer_id int, price "
                    + "double, count int, PRIMARY KEY ((id, day), customer_id, ts))");

    if (supportsSASI(ccmRule) && !isSasiBroken(ccmRule)) {
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
  private static final Version BROKEN_SASI_VERSION = Version.parse("6.8.0");

  protected static boolean isSasiBroken(CcmRule ccmRule) {
    Optional<Version> dseVersion = ccmRule.getDseVersion();
    // creating SASI indexes is broken in DSE 6.8.0
    return dseVersion.isPresent() && dseVersion.get().compareTo(BROKEN_SASI_VERSION) == 0;
  }

  protected static boolean supportsSASI(CcmRule ccmRule) {
    return ccmRule.getCassandraVersion().compareTo(MINIMUM_SASI_VERSION) >= 0
        && !CcmBridge
            .SCYLLA_ENABLEMENT /* @IntegrationTestDisabledScyllaFailure @IntegrationTestDisabledScyllaUnsupportedFunctionality @IntegrationTestDisabledScyllaUnsupportedIndex */;
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
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Product) {
        Product that = (Product) other;
        return Objects.equals(id, that.id)
            && Objects.equals(description, that.description)
            && Objects.equals(dimensions, that.dimensions);
      } else {
        return false;
      }
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
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof ProductWithoutId) {
        ProductWithoutId that = (ProductWithoutId) other;
        return Objects.equals(description, that.description);
      } else {
        return false;
      }
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
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof Dimensions) {
        Dimensions that = (Dimensions) other;
        return this.length == that.length && this.width == that.width && this.height == that.height;
      } else {
        return false;
      }
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
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof OnlyPK) {
        OnlyPK that = (OnlyPK) other;
        return Objects.equals(this.id, that.id);
      } else {
        return false;
      }
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

  @Entity
  public static class ProductSale {
    @PartitionKey private UUID id;

    @PartitionKey(1)
    private String day;

    @ClusteringColumn private int customerId;

    @ClusteringColumn(1)
    private UUID ts;

    private double price;

    private int count;

    public ProductSale() {}

    public ProductSale(UUID id, String day, int customerId, UUID ts, double price, int count) {
      this.id = id;
      this.day = day;
      this.customerId = customerId;
      this.ts = ts;
      this.price = price;
      this.count = count;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public String getDay() {
      return day;
    }

    public void setDay(String day) {
      this.day = day;
    }

    public UUID getTs() {
      return ts;
    }

    public void setTs(UUID ts) {
      this.ts = ts;
    }

    public int getCustomerId() {
      return customerId;
    }

    public void setCustomerId(int customerId) {
      this.customerId = customerId;
    }

    public double getPrice() {
      return price;
    }

    public void setPrice(double price) {
      this.price = price;
    }

    public int getCount() {
      return count;
    }

    public void setCount(int count) {
      this.count = count;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other instanceof ProductSale) {
        ProductSale that = (ProductSale) other;
        return Double.compare(this.price, that.price) == 0
            && this.count == that.count
            && this.id.equals(that.id)
            && this.day.equals(that.day)
            && this.ts.equals(that.ts)
            && this.customerId == that.customerId;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, day, ts, customerId, price, count);
    }

    @Override
    public String toString() {
      return "ProductSale{"
          + "id="
          + id
          + ", day='"
          + day
          + '\''
          + ", customerId="
          + customerId
          + ", ts="
          + ts
          + ", price="
          + price
          + ", count="
          + count
          + '}';
    }
  }
}
