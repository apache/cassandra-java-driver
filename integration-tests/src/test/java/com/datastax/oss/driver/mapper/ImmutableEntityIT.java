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

import static com.datastax.oss.driver.api.mapper.entity.naming.GetterStyle.FLUENT;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.PropertyStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Select;
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
public class ImmutableEntityIT extends InventoryITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ImmutableProductDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query : createStatements(CCM_RULE)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    InventoryMapper mapper = InventoryMapper.builder(session).build();
    dao = mapper.immutableProductDao(SESSION_RULE.keyspace());
  }

  @Test
  public void should_insert_and_retrieve_immutable_entities() {
    ImmutableProduct originalProduct =
        new ImmutableProduct(UUID.randomUUID(), "mock description", new Dimensions(1, 2, 3), -1);
    dao.save(originalProduct);

    ImmutableProduct retrievedProduct = dao.findById(originalProduct.id());
    assertThat(retrievedProduct).isEqualTo(originalProduct);
  }

  @Entity
  @CqlName("product")
  @PropertyStrategy(getterStyle = FLUENT, mutable = false)
  public static class ImmutableProduct {
    @PartitionKey private final UUID id;
    private final String description;
    private final Dimensions dimensions;

    @Computed("writetime(description)")
    private final long writetime;

    public ImmutableProduct(UUID id, String description, Dimensions dimensions, long writetime) {
      this.id = id;
      this.description = description;
      this.dimensions = dimensions;
      this.writetime = writetime;
    }

    public UUID id() {
      return id;
    }

    public String description() {
      return description;
    }

    public Dimensions dimensions() {
      return dimensions;
    }

    public long writetime() {
      return writetime;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof ImmutableProduct) {
        ImmutableProduct that = (ImmutableProduct) other;
        return Objects.equals(this.id, that.id)
            && Objects.equals(this.description, that.description)
            && Objects.equals(this.dimensions, that.dimensions);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description, dimensions);
    }
  }

  @Mapper
  public interface InventoryMapper {
    static MapperBuilder<InventoryMapper> builder(CqlSession session) {
      return new ImmutableEntityIT_InventoryMapperBuilder(session);
    }

    @DaoFactory
    ImmutableProductDao immutableProductDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ImmutableProductDao {
    @Select
    ImmutableProduct findById(UUID productId);

    @Insert
    void save(ImmutableProduct product);
  }
}
