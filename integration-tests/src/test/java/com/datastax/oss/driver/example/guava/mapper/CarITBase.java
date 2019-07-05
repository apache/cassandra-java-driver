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
package com.datastax.oss.driver.example.guava.mapper;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public abstract class CarITBase {
  protected static GuavaSessionGetEntityIT.Car SAAB93 =
      new GuavaSessionGetEntityIT.Car(UUID.randomUUID(), "SAAB", "9-3", 1999);
  protected static GuavaSessionGetEntityIT.Car SAAB900 =
      new GuavaSessionGetEntityIT.Car(UUID.randomUUID(), "SAAB", "900", 1993);

  protected static List<String> createStatements(CcmRule ccmRule) {
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builder()
            .add("CREATE TABLE car(id uuid PRIMARY KEY, make text, model test, year int)");

    return builder.build();
  }

  @Entity
  public static class Car {

    @PartitionKey private UUID id;
    private String make;
    private String model;
    private Integer year;

    public Car() {}

    public Car(UUID id, String make, String model, Integer year) {
      this.id = id;
      this.make = make;
      this.model = model;
      this.year = year;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public String getMake() {
      return make;
    }

    public void setMake(String make) {
      this.make = make;
    }

    public String getModel() {
      return model;
    }

    public void setModel(String model) {
      this.model = model;
    }

    public Integer getYear() {
      return year;
    }

    public void setYear(Integer year) {
      this.year = year;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Car car = (Car) o;
      return Objects.equals(id, car.id)
          && Objects.equals(make, car.make)
          && Objects.equals(model, car.model)
          && Objects.equals(year, car.year);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, make, model, year);
    }

    @Override
    public String toString() {
      return "Car{"
          + "id="
          + id
          + ", make='"
          + make
          + '\''
          + ", model='"
          + model
          + '\''
          + ", year="
          + year
          + '}';
    }
  }
}
