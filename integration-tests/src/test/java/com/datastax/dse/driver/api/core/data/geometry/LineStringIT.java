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
package com.datastax.dse.driver.api.core.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import java.util.List;
import java.util.UUID;
import org.assertj.core.util.Lists;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BackendRequirement(type = BackendType.DSE, minInclusive = "5.0")
public class LineStringIT extends GeometryIT<LineString> {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static final String LINE_STRING_TYPE = "LineStringType";

  public LineStringIT() {
    super(
        Lists.newArrayList(
            LineString.fromPoints(Point.fromCoordinates(0, 10), Point.fromCoordinates(10, 0)),
            LineString.fromPoints(
                Point.fromCoordinates(30, 10),
                Point.fromCoordinates(10, 30),
                Point.fromCoordinates(40, 40)),
            LineString.fromPoints(
                Point.fromCoordinates(-5, 0),
                Point.fromCoordinates(0, 10),
                Point.fromCoordinates(10, 5))),
        LineString.class,
        sessionRule);
  }

  @BeforeClass
  public static void initialize() {
    onTestContextInitialized(LINE_STRING_TYPE, sessionRule);
  }

  @Test
  public void should_insert_and_retrieve_empty_linestring() {
    LineString empty = LineString.fromWellKnownText("LINESTRING EMPTY");
    UUID key = Uuids.random();
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("INSERT INTO tbl (k, g) VALUES (?, ?)")
                .addPositionalValues(key, empty)
                .build());

    ResultSet result =
        sessionRule
            .session()
            .execute(
                SimpleStatement.builder("SELECT g from tbl where k=?")
                    .addPositionalValues(key)
                    .build());
    Row row = result.iterator().next();
    List<Point> points = row.get("g", LineString.class).getPoints();
    assertThat(points.isEmpty()).isTrue();
  }
}
