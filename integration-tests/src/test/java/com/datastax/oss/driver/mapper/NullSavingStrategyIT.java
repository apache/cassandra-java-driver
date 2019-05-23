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
package com.datastax.oss.driver.mapper;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Objects;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class NullSavingStrategyIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccm)
          .withConfigLoader(
              DriverConfigLoader.programmaticBuilder()
                  .withString(DefaultDriverOption.PROTOCOL_VERSION, "V3")
                  .build())
          .build();

  private static InventoryMapper mapper;

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE product_simple(id uuid PRIMARY KEY, description text)")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    mapper = new NullSavingStrategyIT_InventoryMapperBuilder(session).build();
  }

  @Test
  public void should_throw_when_try_to_construct_dao_with_DO_NOT_SET_strategy_for_V3_protocol() {
    assertThatThrownBy(() -> mapper.productDao(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("You cannot use NullSavingStrategy.DO_NOT_SET for protocol version V3.");
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductSimpleDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductSimpleDao {

    @Update(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void update(ProductSimple product);

    @Select
    ProductSimple findById(UUID productId);
  }

  @Entity
  public static class ProductSimple {
    @PartitionKey private UUID id;
    private String description;

    public ProductSimple() {}

    public ProductSimple(UUID id, String description) {
      this.id = id;
      this.description = description;
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

    @Override
    public boolean equals(Object o) {

      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductSimple that = (ProductSimple) o;
      return Objects.equals(id, that.id) && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description);
    }

    @Override
    public String toString() {
      return "ProductSimple{" + "id=" + id + ", description='" + description + '\'' + '}';
    }
  }
}
