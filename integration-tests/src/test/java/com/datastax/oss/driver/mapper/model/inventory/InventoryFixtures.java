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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.mapper.model.EntityFixture;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.UUID;

public class InventoryFixtures {
  public static List<String> createStatements() {
    return ImmutableList.of(
        "CREATE TYPE dimensions(length int, width int, height int)",
        "CREATE TABLE product(id uuid PRIMARY KEY, description text, dimensions dimensions)");
  }

  public static EntityFixture<Product> FLAMETHROWER =
      new EntityFixture<Product>(
          new Product(UUID.randomUUID(), "Flamethrower", new Dimensions(30, 10, 8))) {

        @Override
        public void assertMatches(GettableByName data) {
          assertThat(data.getUuid("id")).isEqualTo(entity.getId());
          assertThat(data.getString("description")).isEqualTo(entity.getDescription());
          UdtValue udtValue = data.getUdtValue("dimensions");
          assertThat(udtValue.getType().getName().asInternal()).isEqualTo("dimensions");
          assertThat(udtValue.getInt("length")).isEqualTo(entity.getDimensions().getLength());
          assertThat(udtValue.getInt("width")).isEqualTo(entity.getDimensions().getWidth());
          assertThat(udtValue.getInt("height")).isEqualTo(entity.getDimensions().getHeight());
        }
      };

  /** Has a null entity property. */
  public static EntityFixture<Product> MP3_DOWNLOAD =
      new EntityFixture<Product>(new Product(UUID.randomUUID(), "MP3 download", null)) {
        @Override
        public void assertMatches(GettableByName data) {
          assertThat(data.getUuid("id")).isEqualTo(entity.getId());
          assertThat(data.getString("description")).isEqualTo(entity.getDescription());
          UdtValue udtValue = data.getUdtValue("dimensions");
          assertThat(udtValue.getType().getName().asInternal()).isEqualTo("dimensions");
          assertThat(udtValue.isNull("length")).isTrue();
          assertThat(udtValue.isNull("width")).isTrue();
          assertThat(udtValue.isNull("height")).isTrue();
        }
      };

  public static EntityFixture<Dimensions> SAMPLE_DIMENSIONS =
      new EntityFixture<Dimensions>(new Dimensions(30, 10, 8)) {
        @Override
        public void assertMatches(GettableByName data) {
          assertThat(data.getInt("length")).isEqualTo(entity.getLength());
          assertThat(data.getInt("width")).isEqualTo(entity.getWidth());
          assertThat(data.getInt("height")).isEqualTo(entity.getHeight());
        }
      };
}
