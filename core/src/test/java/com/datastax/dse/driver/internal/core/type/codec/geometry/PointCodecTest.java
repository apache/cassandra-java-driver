/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.type.codec.geometry;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class PointCodecTest extends GeometryCodecTest<Point, PointCodec> {

  public PointCodecTest() {
    super(new PointCodec());
  }

  @DataProvider
  public static Object[][] serde() {
    return new Object[][] {
      {null, null},
      {Point.fromCoordinates(1, 2), Point.fromCoordinates(1, 2)},
      {Point.fromCoordinates(-1.1, -2.2), Point.fromCoordinates(-1.1, -2.2)}
    };
  }

  @DataProvider
  public static Object[][] format() {
    return new Object[][] {
      {null, "NULL"},
      {Point.fromCoordinates(1, 2), "'POINT (1 2)'"},
      {Point.fromCoordinates(-1.1, -2.2), "'POINT (-1.1 -2.2)'"}
    };
  }

  @DataProvider
  public static Object[][] parse() {
    return new Object[][] {
      {null, null},
      {"", null},
      {" ", null},
      {"NULL", null},
      {" NULL ", null},
      {"'POINT ( 1 2 )'", Point.fromCoordinates(1, 2)},
      {"'POINT ( 1.0 2.0 )'", Point.fromCoordinates(1, 2)},
      {"' point ( -1.1 -2.2 )'", Point.fromCoordinates(-1.1, -2.2)},
      {" ' Point ( -1.1 -2.2 ) ' ", Point.fromCoordinates(-1.1, -2.2)}
    };
  }

  @Test
  @UseDataProvider("format")
  @Override
  public void should_format(Point input, String expected) {
    super.should_format(input, expected);
  }

  @Test
  @UseDataProvider("parse")
  @Override
  public void should_parse(String input, Point expected) {
    super.should_parse(input, expected);
  }
}
