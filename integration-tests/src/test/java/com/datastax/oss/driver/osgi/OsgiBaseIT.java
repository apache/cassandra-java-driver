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
package com.datastax.oss.driver.osgi;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.categories.IsolatedTests;
import java.util.List;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;

/**
 * Tests the capability of using the driver in an OSGi environment. Note that this relies on
 * relative locations of jars in the target directory of their respective modules. It is therefore
 * required that you at least run {@code mvn package} before running these tests.
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
@Category(IsolatedTests.class)
public abstract class OsgiBaseIT {

  @ClassRule public static CustomCcmRule ccmRule = CustomCcmRule.builder().withNodes(1).build();

  /** @return config loader to be used to create session. */
  protected abstract DriverConfigLoader configLoader();

  /**
   * A very simple test that ensures a session can be established and a query made when running in
   * an OSGi container.
   */
  @Test
  public void should_connect_and_query() {
    SessionBuilder<CqlSessionBuilder, CqlSession> builder =
        CqlSession.builder()
            .addContactEndPoints(ccmRule.getContactPoints())
            // use the driver's ClassLoader instead of the OSGI application thread's.
            .withClassLoader(CqlSession.class.getClassLoader())
            .withConfigLoader(configLoader());
    try (CqlSession session = builder.build()) {
      ResultSet result = session.execute(selectFrom("system", "local").all().build());

      List<Row> rows = result.all();
      assertThat(rows).hasSize(1);

      Row row = rows.get(0);
      assertThat(row.getString("key")).isEqualTo("local");
    }
  }
}
