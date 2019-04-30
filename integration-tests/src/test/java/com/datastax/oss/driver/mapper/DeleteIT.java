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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.mapper.model.inventory.InventoryFixtures;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapper;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapperBuilder;
import com.datastax.oss.driver.mapper.model.inventory.Product;
import com.datastax.oss.driver.mapper.model.inventory.ProductDao;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class DeleteIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao productDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : InventoryFixtures.createStatements()) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
    productDao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void insertFixtures() {
    productDao.save(InventoryFixtures.FLAMETHROWER.entity);
    productDao.save(InventoryFixtures.MP3_DOWNLOAD.entity);
  }

  @Test
  public void should_delete_entity() {
    Product entity = InventoryFixtures.FLAMETHROWER.entity;
    UUID id = entity.getId();
    productDao.delete(entity);
    assertThat(productDao.findById(id)).isNull();
  }

  @Test
  public void should_delete_by_id() {
    Product entity = InventoryFixtures.FLAMETHROWER.entity;
    UUID id = entity.getId();
    productDao.deleteById(id);
    assertThat(productDao.findById(id)).isNull();

    // Non-existing id should be silently ignored
    productDao.deleteById(id);
  }

  @Test
  public void should_delete_if_exists() {
    Product entity = InventoryFixtures.FLAMETHROWER.entity;
    UUID id = entity.getId();
    assertThat(productDao.deleteIfExists(entity)).isTrue();
    assertThat(productDao.findById(id)).isNull();
    assertThat(productDao.deleteIfExists(entity)).isFalse();
  }

  @Test
  public void should_delete_with_condition() {
    Product entity = InventoryFixtures.FLAMETHROWER.entity;
    UUID id = entity.getId();
    ResultSet rs = productDao.deleteIfDescriptionMatches(id, "foo");
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.one().getString("description")).isEqualTo(entity.getDescription());

    rs = productDao.deleteIfDescriptionMatches(id, entity.getDescription());
    assertThat(rs.wasApplied()).isTrue();
    assertThat(productDao.findById(id)).isNull();
  }
}
