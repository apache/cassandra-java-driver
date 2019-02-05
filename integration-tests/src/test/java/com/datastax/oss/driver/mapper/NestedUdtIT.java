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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.mapper.model.udts.Container;
import com.datastax.oss.driver.mapper.model.udts.ContainerDao;
import com.datastax.oss.driver.mapper.model.udts.UdtsFixtures;
import com.datastax.oss.driver.mapper.model.udts.UdtsMapper;
import com.datastax.oss.driver.mapper.model.udts.UdtsMapperBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class NestedUdtIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ContainerDao containerDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : UdtsFixtures.createStatements()) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    UdtsMapper udtsMapper = new UdtsMapperBuilder(session).build();
    containerDao = udtsMapper.containerDao(sessionRule.keyspace());
  }

  @Test
  public void should_insert_entity_with_nested_udts() {
    // Given
    CqlSession session = sessionRule.session();

    // When
    Container container = UdtsFixtures.SAMPLE_CONTAINER.entity;
    containerDao.save(container);
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT * FROM container WHERE id = ?", container.getId()))
            .one();

    // Then
    UdtsFixtures.SAMPLE_CONTAINER.assertMatches(row);
  }

  @Test
  public void should_get_entity_with_nested_udts() {
    // Given
    CqlSession session = sessionRule.session();

    // When
    Container insertedContainer = UdtsFixtures.SAMPLE_CONTAINER.entity;
    containerDao.save(insertedContainer);
    // TODO maybe replace with a Dao @Select once that is implemented
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT * FROM container WHERE id = ?", insertedContainer.getId()))
            .one();
    Container retrievedContainer = containerDao.get(row);

    // Then
    assertThat(retrievedContainer).isEqualTo(insertedContainer);
  }
}
