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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class SetEntityIT extends InventoryITBase {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : createStatements()) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new SetEntityIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Test
  public void should_set_entity_on_bound_statement() {
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatement boundStatement = preparedStatement.bind();

    boundStatement = dao.set(FLAMETHROWER, boundStatement);

    assertMatches(boundStatement, FLAMETHROWER);
  }

  @Test
  public void should_set_entity_on_bound_statement_builder() {
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    dao.set(builder, FLAMETHROWER);
    BoundStatement boundStatement = builder.build();

    assertMatches(boundStatement, FLAMETHROWER);
  }

  @Test
  public void should_set_entity_on_udt_value() {
    CqlSession session = sessionRule.session();
    UserDefinedType udtType =
        session
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
            .orElseThrow(AssertionError::new)
            .getUserDefinedType("dimensions")
            .orElseThrow(AssertionError::new);
    UdtValue udtValue = udtType.newValue();
    Dimensions dimensions = new Dimensions(30, 10, 8);

    dao.set(dimensions, udtValue);

    assertThat(udtValue.getInt("length")).isEqualTo(dimensions.getLength());
    assertThat(udtValue.getInt("width")).isEqualTo(dimensions.getWidth());
    assertThat(udtValue.getInt("height")).isEqualTo(dimensions.getHeight());
  }

  private static void assertMatches(GettableByName data, Product entity) {
    assertThat(data.getUuid("id")).isEqualTo(entity.getId());
    assertThat(data.getString("description")).isEqualTo(entity.getDescription());
    UdtValue udtValue = data.getUdtValue("dimensions");
    assertThat(udtValue.getType().getName().asInternal()).isEqualTo("dimensions");
    assertThat(udtValue.getInt("length")).isEqualTo(entity.getDimensions().getLength());
    assertThat(udtValue.getInt("width")).isEqualTo(entity.getDimensions().getWidth());
    assertThat(udtValue.getInt("height")).isEqualTo(entity.getDimensions().getHeight());
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductDao {

    @SetEntity
    BoundStatement set(Product product, BoundStatement boundStatement);

    @SetEntity
    void set(BoundStatementBuilder builder, Product product);

    @SetEntity
    void set(Dimensions dimensions, UdtValue udtValue);
  }
}
