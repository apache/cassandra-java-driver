/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.data.geometry;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import org.assertj.core.util.Lists;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "5.0")
public class PointIT extends GeometryIT<Point> {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<DseSession> sessionRule = new DseSessionRuleBuilder(ccm).build();

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
