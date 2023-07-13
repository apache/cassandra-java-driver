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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import org.assertj.core.util.Lists;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BackendRequirement(type = BackendType.DSE, minInclusive = "5.0")
public class PointIT extends GeometryIT<Point> {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static final String POINT_TYPE = "PointType";

  public PointIT() {
    super(
        Lists.newArrayList(
            Point.fromCoordinates(-1.0, -5),
            Point.fromCoordinates(0, 0),
            Point.fromCoordinates(1.1, 2.2),
            Point.fromCoordinates(Double.MIN_VALUE, 0),
            Point.fromCoordinates(Double.MAX_VALUE, Double.MIN_VALUE)),
        Point.class,
        sessionRule);
  }

  @BeforeClass
  public static void initialize() {
    onTestContextInitialized(POINT_TYPE, sessionRule);
  }
}
