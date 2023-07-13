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
package com.datastax.oss.driver.osgi.support;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.column;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.querybuilder.DseSchemaBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.reactivex.Flowable;

public interface OsgiReactiveTests extends OsgiSimpleTests {

  /**
   * Ensures a session can be established and a query using Reactive can be made when running in an
   * OSGi container.
   */
  default void connectAndQueryReactive() {

    try (CqlSession session = sessionBuilder().build()) {

      Flowable.fromPublisher(
              session.executeReactive(String.format(CREATE_KEYSPACE, "test_osgi_reactive")))
          .blockingSubscribe();

      // test that ESRI is available
      Flowable.fromPublisher(
              session.executeReactive(
                  // also exercise the DSE query builder
                  DseSchemaBuilder.createTable("test_osgi_reactive", "t1")
                      .ifNotExists()
                      .withPartitionKey("pk", DataTypes.INT)
                      .withColumn("v", DataTypes.INT)
                      .build()))
          .blockingSubscribe();

      Flowable.fromPublisher(
              session.executeReactive(
                  SimpleStatement.newInstance(
                      "INSERT INTO test_osgi_reactive.t1 (pk, v) VALUES (0, 1)")))
          .blockingSubscribe();

      Row row =
          Flowable.fromPublisher(
                  session.executeReactive(
                      // test that the Query Builder is available
                      selectFrom("test_osgi_reactive", "t1")
                          .column("v")
                          .where(column("pk").isEqualTo(literal(0)))
                          .build()))
              .blockingFirst();

      assertThat(row).isNotNull();
      assertThat(row.getInt(0)).isEqualTo(1);
    }
  }
}
