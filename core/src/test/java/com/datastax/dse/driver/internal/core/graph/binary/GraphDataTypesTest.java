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
package com.datastax.dse.driver.internal.core.graph.binary;

import static com.datastax.oss.driver.api.core.type.DataTypes.BIGINT;
import static com.datastax.oss.driver.api.core.type.DataTypes.DOUBLE;
import static com.datastax.oss.driver.api.core.type.DataTypes.DURATION;
import static com.datastax.oss.driver.api.core.type.DataTypes.FLOAT;
import static com.datastax.oss.driver.api.core.type.DataTypes.INT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.core.type.DataTypes.listOf;
import static com.datastax.oss.driver.api.core.type.DataTypes.mapOf;
import static com.datastax.oss.driver.api.core.type.DataTypes.setOf;
import static com.datastax.oss.driver.api.core.type.DataTypes.tupleOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import com.datastax.dse.driver.internal.core.graph.EditDistance;
import com.datastax.dse.driver.internal.core.graph.GraphConversions;
import com.datastax.dse.driver.internal.core.graph.GraphProtocol;
import com.datastax.dse.driver.internal.core.graph.GraphSONUtils;
import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class GraphDataTypesTest {

  private GraphBinaryModule graphBinaryModule;

  @Mock private DefaultDriverContext context;

  private static final MutableCodecRegistry CODEC_REGISTRY =
      new DefaultCodecRegistry("testDseRegistry");

  static {
    CODEC_REGISTRY.register(DseTypeCodecs.POINT, DseTypeCodecs.LINE_STRING, DseTypeCodecs.POLYGON);
  }

  private static Object[][] graphsonOneDataTypes =
      new Object[][] {
        {"~’~^ää#123#ö"},
        {(byte) 34},
        {BigDecimal.TEN},
        {BigInteger.TEN},
        {Boolean.TRUE},
        {false},
        {23},
        {23L},
        {23.0d},
        {23f},
        {(short) 23},
        {LocalDate.now(ZoneOffset.UTC)},
        {LocalTime.now(ZoneOffset.UTC)},
        {java.util.UUID.randomUUID()},
        {Instant.now()},
      };

  private static Object[][] graphsonTwoDataTypes =
      new Object[][] {
        {ImmutableList.of(1L, 2L, 3L)},
        {ImmutableSet.of(1L, 2L, 3L)},
        {ImmutableMap.of("a", 1, "b", 2)},
        {Point.fromCoordinates(3.3, 4.4)},
        {
          LineString.fromPoints(
              Point.fromCoordinates(1, 1), Point.fromCoordinates(2, 2), Point.fromCoordinates(3, 3))
        },
        {
          Polygon.fromPoints(
              Point.fromCoordinates(3, 4), Point.fromCoordinates(5, 4), Point.fromCoordinates(6, 6))
        },
      };

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(context.getCodecRegistry()).thenReturn(CODEC_REGISTRY);
    when(context.getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V2);

    TypeSerializerRegistry registry = GraphBinaryModule.createDseTypeSerializerRegistry(context);
    graphBinaryModule =
        new GraphBinaryModule(new GraphBinaryReader(registry), new GraphBinaryWriter(registry));
  }

  @DataProvider
  public static Object[][] graphsonOneDataProvider() {

    return graphsonOneDataTypes;
  }

  @DataProvider
  public static Object[][] graphsonTwoDataProvider() {

    return TestDataProviders.concat(graphsonOneDataTypes, graphsonTwoDataTypes);
  }

  @DataProvider
  public static Object[][] binaryDataProvider() throws UnknownHostException {

    Object[][] binaryDataTypes =
        new Object[][] {
          {InetAddress.getLocalHost()},
          {ImmutableList.of(ImmutableList.of(1L, 3L), ImmutableList.of(2L, 4L))},
          {ImmutableSet.of(ImmutableSet.of(1, 2, 3))},
          {ImmutableMap.of(ImmutableMap.of("a", 1), ImmutableMap.of(2, "b"))},
          {tupleOf(INT, TEXT, FLOAT).newValue(1, "2", 3.41f)},
          {
            tupleOf(INT, TEXT, tupleOf(TEXT, DURATION))
                .newValue(
                    1, "2", tupleOf(TEXT, DURATION).newValue("a", CqlDuration.newInstance(2, 1, 0)))
          },
          {
            tupleOf(
                    listOf(INT),
                    setOf(FLOAT),
                    DataTypes.mapOf(TEXT, BIGINT),
                    listOf(listOf(DOUBLE)),
                    setOf(setOf(FLOAT)),
                    listOf(tupleOf(INT, TEXT)))
                .newValue(
                    ImmutableList.of(4, 8, 22, 34, 37, 59),
                    ImmutableSet.of(28f, 44f, 59f),
                    ImmutableMap.of("big10", 2345L),
                    ImmutableList.of(
                        ImmutableList.of(11.1d, 33.3d), ImmutableList.of(22.2d, 44.4d)),
                    ImmutableSet.of(ImmutableSet.of(55.5f)),
                    ImmutableList.of(tupleOf(INT, TEXT).newValue(3, "three")))
          },
          {
            new UserDefinedTypeBuilder("ks", "udt1")
                .withField("a", INT)
                .withField("b", TEXT)
                .build()
                .newValue(1, "two")
          },
          {new Distance(Point.fromCoordinates(3.4, 17.0), 2.5)},
          {new EditDistance("xyz", 3)},
          {DseGraph.g.V().has("name", "marko").asAdmin().getBytecode()},
          {
            GraphConversions.bytecodeToSerialize(
                BatchGraphStatement.builder()
                    .addTraversal(DseGraph.g.addV("person").property("name", "1"))
                    .addTraversal(DseGraph.g.addV("person").property("name", "1"))
                    .build())
          },
        };
    return TestDataProviders.concat(graphsonTwoDataProvider(), binaryDataTypes);
  }

  @Test
  @UseDataProvider("binaryDataProvider")
  public void dataTypesTest(Object value) throws IOException {
    verifySerDeBinary(value);
  }

  @Test
  @UseDataProvider("graphsonOneDataProvider")
  public void dataTypesTestGraphsonOne(Object value) throws IOException {
    verifySerDeGraphson(value, GraphProtocol.GRAPHSON_1_0);
  }

  @Test
  @UseDataProvider("graphsonTwoDataProvider")
  public void dataTypesTestGraphsonTwo(Object value) throws IOException {
    verifySerDeGraphson(value, GraphProtocol.GRAPHSON_2_0);
  }

  @Test
  public void complexUdtTests() throws IOException {
    UserDefinedType type1 =
        new UserDefinedTypeBuilder("ks", "udt1").withField("a", INT).withField("b", TEXT).build();
    verifySerDeBinary(type1.newValue(1, "2"));

    TupleType secondNested = tupleOf(BIGINT, listOf(BIGINT));
    TupleType firstNested = tupleOf(TEXT, secondNested);

    UserDefinedType type2 =
        new UserDefinedTypeBuilder("ks", "udt2")
            .withField("a", INT)
            .withField("b", TEXT)
            .withField("c", type1)
            .withField("mylist", listOf(BIGINT))
            .withField("mytuple_withlist", firstNested)
            .build();

    verifySerDeBinary(
        type2.newValue(
            1,
            "2",
            type1.newValue(3, "4"),
            ImmutableList.of(5L),
            firstNested.newValue("6", secondNested.newValue(7L, ImmutableList.of(8L)))));

    UserDefinedType type3 =
        new UserDefinedTypeBuilder("ks", "udt3")
            .withField("a", listOf(INT))
            .withField("b", setOf(FLOAT))
            .withField("c", mapOf(TEXT, BIGINT))
            .withField("d", listOf(listOf(DOUBLE)))
            .withField("e", setOf(setOf(FLOAT)))
            .withField("f", listOf(tupleOf(INT, TEXT)))
            .build();

    verifySerDeBinary(
        type3.newValue(
            ImmutableList.of(1),
            ImmutableSet.of(2.1f),
            ImmutableMap.of("3", 4L),
            ImmutableList.of(ImmutableList.of(5.1d, 6.1d), ImmutableList.of(7.1d)),
            ImmutableSet.of(ImmutableSet.of(8.1f), ImmutableSet.of(9.1f)),
            ImmutableList.of(tupleOf(INT, TEXT).newValue(10, "11"))));
  }

  @Test
  public void complexTypesAndGeoTests() throws IOException {

    TupleType tuple = tupleOf(DseDataTypes.POINT, DseDataTypes.LINE_STRING, DseDataTypes.POLYGON);
    tuple.attach(context);

    verifySerDeBinary(
        tuple.newValue(
            Point.fromCoordinates(3.3, 4.4),
            LineString.fromPoints(
                Point.fromCoordinates(1, 1),
                Point.fromCoordinates(2, 2),
                Point.fromCoordinates(3, 3)),
            Polygon.fromPoints(
                Point.fromCoordinates(3, 4),
                Point.fromCoordinates(5, 4),
                Point.fromCoordinates(6, 6))));

    UserDefinedType udt =
        new UserDefinedTypeBuilder("ks", "udt1")
            .withField("a", DseDataTypes.POINT)
            .withField("b", DseDataTypes.LINE_STRING)
            .withField("c", DseDataTypes.POLYGON)
            .build();
    udt.attach(context);

    verifySerDeBinary(
        udt.newValue(
            Point.fromCoordinates(3.3, 4.4),
            LineString.fromPoints(
                Point.fromCoordinates(1, 1),
                Point.fromCoordinates(2, 2),
                Point.fromCoordinates(3, 3)),
            Polygon.fromPoints(
                Point.fromCoordinates(3, 4),
                Point.fromCoordinates(5, 4),
                Point.fromCoordinates(6, 6))));
  }

  private void verifySerDeBinary(Object input) throws IOException {
    Buffer result = graphBinaryModule.serialize(input);
    Object deserialized = graphBinaryModule.deserialize(result);
    result.release();
    assertThat(deserialized).isEqualTo(input);
  }

  private void verifySerDeGraphson(Object input, GraphProtocol protocol) throws IOException {
    ByteBuffer buffer = GraphSONUtils.serializeToByteBuffer(input, protocol);
    Object deserialized = deserializeGraphson(buffer, protocol, input.getClass());

    Object expected = (input instanceof Set) ? ImmutableList.copyOf((Set<?>) input) : input;
    assertThat(deserialized).isEqualTo(expected);
  }

  private Object deserializeGraphson(
      ByteBuffer buffer, GraphProtocol protocol, Class<?> expectedClass) throws IOException {
    List<ByteBuffer> data = ImmutableList.of(buffer);
    GraphNode node = GraphSONUtils.createGraphNode(data, protocol);
    return node.as(expectedClass);
  }
}
