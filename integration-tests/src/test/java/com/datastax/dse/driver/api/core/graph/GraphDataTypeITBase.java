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
package com.datastax.dse.driver.api.core.graph;

import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public abstract class GraphDataTypeITBase {

  private static final boolean IS_DSE50 = CcmBridge.VERSION.compareTo(Version.parse("5.1")) < 0;
  private static final Set<String> TYPES_REQUIRING_DSE51 =
      ImmutableSet.of("Date()", "Time()", "Point()", "Linestring()", "Polygon()");

  private static AtomicInteger schemaCounter = new AtomicInteger();

  @DataProvider
  public static Object[][] typeSamples() {
    return new Object[][] {
      // Types that DSE supports.
      {"Boolean()", true},
      {"Boolean()", false},
      {"Smallint()", Short.MAX_VALUE},
      {"Smallint()", Short.MIN_VALUE},
      {"Smallint()", (short) 0},
      {"Smallint()", (short) 42},
      {"Int()", Integer.MAX_VALUE},
      {"Int()", Integer.MIN_VALUE},
      {"Int()", 0},
      {"Int()", 42},
      {"Bigint()", Long.MAX_VALUE},
      {"Bigint()", Long.MIN_VALUE},
      {"Bigint()", 0L},
      {"Double()", Double.MAX_VALUE},
      {"Double()", Double.MIN_VALUE},
      {"Double()", 0.0d},
      {"Double()", Math.PI},
      {"Float()", Float.MAX_VALUE},
      {"Float()", Float.MIN_VALUE},
      {"Float()", 0.0f},
      {"Text()", ""},
      {"Text()", "75"},
      {"Text()", "Lorem Ipsum"},
      // Inet, UUID, Date
      {"Inet()", InetAddresses.forString("127.0.0.1")},
      {"Inet()", InetAddresses.forString("0:0:0:0:0:0:0:1")},
      {"Inet()", InetAddresses.forString("2001:db8:85a3:0:0:8a2e:370:7334")},
      {"Uuid()", UUID.randomUUID()},
      // Timestamps
      {"Timestamp()", Instant.ofEpochMilli(123)},
      {"Timestamp()", Instant.ofEpochMilli(1488313909)},
      {"Duration()", java.time.Duration.parse("P2DT3H4M")},
      {"Date()", LocalDate.of(2016, 5, 12)},
      {"Time()", LocalTime.parse("18:30:41.554")},
      {"Time()", LocalTime.parse("18:30:41.554010034")},
      // Blob
      {"Blob()", "Hello World!".getBytes(Charsets.UTF_8)},
      // BigDecimal/BigInteger
      {"Decimal()", new BigDecimal("8675309.9998")},
      {"Varint()", new BigInteger("8675309")},
      // Geospatial types
      {"Point().withBounds(-2, -2, 2, 2)", Point.fromCoordinates((double) 0, (double) 1)},
      {"Point().withBounds(-40, -40, 40, 40)", Point.fromCoordinates((double) -5, (double) 20)},
      {
        "Linestring().withGeoBounds()",
        LineString.fromPoints(
            Point.fromCoordinates((double) 30, (double) 10),
            Point.fromCoordinates((double) 10, (double) 30),
            Point.fromCoordinates((double) 40, (double) 40))
      },
      {
        "Polygon().withGeoBounds()",
        Polygon.builder()
            .addRing(
                Point.fromCoordinates((double) 35, (double) 10),
                Point.fromCoordinates((double) 45, (double) 45),
                Point.fromCoordinates((double) 15, (double) 40),
                Point.fromCoordinates((double) 10, (double) 20),
                Point.fromCoordinates((double) 35, (double) 10))
            .addRing(
                Point.fromCoordinates((double) 20, (double) 30),
                Point.fromCoordinates((double) 35, (double) 35),
                Point.fromCoordinates((double) 30, (double) 20),
                Point.fromCoordinates((double) 20, (double) 30))
            .build()
      }
    };
  }

  @UseDataProvider("typeSamples")
  @Test
  public void should_create_and_retrieve_vertex_property_with_correct_type(
      String type, Object value) {
    if (IS_DSE50 && requiresDse51(type)) {
      throw new AssumptionViolatedException(type + " not supported in DSE " + CcmBridge.VERSION);
    }

    int id = schemaCounter.getAndIncrement();

    String vertexLabel = "vertex" + id;
    String propertyName = "prop" + id;
    GraphStatement addVertexLabelAndProperty =
        ScriptGraphStatement.builder(
                "schema.propertyKey(property)."
                    + type
                    + ".create()\n"
                    + "schema.vertexLabel(vertexLabel).properties(property).create()")
            .setQueryParam("vertexLabel", vertexLabel)
            .setQueryParam("property", propertyName)
            .build();

    session().execute(addVertexLabelAndProperty);

    Vertex v = insertVertexAndReturn(vertexLabel, propertyName, value);

    assertThat(v).hasProperty(propertyName, value);
  }

  private boolean requiresDse51(String type) {
    for (String prefix : TYPES_REQUIRING_DSE51) {
      if (type.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  public abstract Vertex insertVertexAndReturn(
      String vertexLabel, String propertyName, Object value);

  /**
   * Note that the {@link SessionRule} (and setupSchema method) must be redeclared in each subclass,
   * since it depends on the CCM rule that can't be shared across serial tests.
   */
  public abstract CqlSession session();
}
