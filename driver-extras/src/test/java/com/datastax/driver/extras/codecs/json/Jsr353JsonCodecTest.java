/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.extras.codecs.json;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.json.*;
import java.io.IOException;
import java.io.StringWriter;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class Jsr353JsonCodecTest extends CCMTestsSupport {

    private static final Jsr353JsonCodec jsonCodec = new Jsr353JsonCodec();

    private static final JsonObject alice = Json.createObjectBuilder().add("id", 1).add("name", "Alice").build();
    private static final JsonObject bob = Json.createObjectBuilder().add("id", 2).add("name", "Bob").build();
    private static final JsonObject charlie = Json.createObjectBuilder().add("id", 3).add("name", "Charlie").build();

    private static final JsonArray bobAndCharlie = Json.createArrayBuilder().add(bob).add(charlie).build();

    private static final String insertQuery = "INSERT INTO t1 (c1, c2) VALUES (?, ?)";
    private static final String selectQuery = "SELECT c1, c2 FROM t1 WHERE c1 = ? and c2 = ?";

    private static final String notAJsonString = "this text is not json";

    private static final String TABLE1 = "test1";
    private static final String TABLE2 = "test2";

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE t1 (c1 text, c2 text, PRIMARY KEY (c1, c2))",
                String.format("CREATE TABLE %s (k text, \"miXeD\" text, i int, f float, PRIMARY KEY (k, \"miXeD\"))", TABLE1),
                insertInto(TABLE1).value("k", "key0").value(quote("miXeD"), "a").value("i", 1).value("f", 1.1).toString(),
                insertInto(TABLE1).value("k", "key0").value(quote("miXeD"), "b").value("i", 2).value("f", 2.5).toString(),
                insertInto(TABLE1).value("k", "key0").value(quote("miXeD"), "c").value("i", 3).value("f", 3.7).toString(),
                insertInto(TABLE1).value("k", "key0").value(quote("miXeD"), "d").value("i", 4).value("f", 5.0).toString()
        );

        if (ccm().getProtocolVersion().compareTo(ProtocolVersion.V3) >= 0) {
            execute(
                    "CREATE TYPE address (street text, zipcode int, phones list<text>)",
                    String.format("CREATE TABLE %s (k text PRIMARY KEY, v frozen<address>)", TABLE2)
            );
        }
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(
                new CodecRegistry().register(jsonCodec) // global JsonStructure <-> varchar codec
        );
    }

    @DataProvider(name = "Jsr353JsonCodecTest")
    public static Object[][] parameters() {
        return new Object[][]{
                {alice},
                {bobAndCharlie}
        };
    }

    @Test(groups = "unit")
    public void test_cql_text_to_json() {
        Jsr353JsonCodec codec = new Jsr353JsonCodec();
        String json = "'{\"id\":1,\"name\":\"John Doe\"}'";
        JsonObject object = Json.createObjectBuilder().add("id", 1).add("name", "John Doe").build();
        assertThat(codec.format(object)).isEqualTo(json);
        assertThat(codec.parse(json)).isEqualTo(object);
    }

    @Test(groups = "unit")
    public void test_nulls() {
        Jsr353JsonCodec codec = new Jsr353JsonCodec();
        assertThat(codec.format(null)).isEqualTo("NULL");
        assertThat(codec.parse(null)).isNull();
    }

    @Test(groups = "short", dataProvider = "Jsr353JsonCodecTest")
    @CassandraVersion("2.0.0")
    public void should_use_custom_codec_with_simple_statements(JsonStructure object) throws IOException {
        session().execute(insertQuery, notAJsonString, object);
        ResultSet rows = session().execute(selectQuery, notAJsonString, object);
        Row row = rows.one();
        assertRow(row, object);
    }

    @Test(groups = "short", dataProvider = "Jsr353JsonCodecTest")
    public void should_use_custom_codec_with_built_statements_1(JsonStructure object) throws IOException {
        BuiltStatement insertStmt = insertInto("t1")
                .value("c1", bindMarker())
                .value("c2", bindMarker());
        BuiltStatement selectStmt = select("c1", "c2")
                .from("t1")
                .where(eq("c1", bindMarker()))
                .and(eq("c2", bindMarker()));
        session().execute(session().prepare(insertStmt).bind(notAJsonString, object));
        ResultSet rows = session().execute(session().prepare(selectStmt).bind(notAJsonString, object));
        Row row = rows.one();
        assertRow(row, object);
    }

    @Test(groups = "short", dataProvider = "Jsr353JsonCodecTest")
    public void should_use_custom_codec_with_built_statements_2(JsonStructure object) throws IOException {
        BuiltStatement insertStmt = insertInto("t1")
                .value("c1", notAJsonString)
                .value("c2", object);
        BuiltStatement selectStmt =
                select("c1", "c2")
                        .from("t1")
                        .where(eq("c1", notAJsonString))
                        .and(eq("c2", object));
        session().execute(insertStmt);
        ResultSet rows = session().execute(selectStmt);
        Row row = rows.one();
        assertRow(row, object);
    }

    @Test(groups = "short", dataProvider = "Jsr353JsonCodecTest")
    public void should_use_custom_codec_with_prepared_statements_1(JsonStructure object) throws IOException {
        session().execute(session().prepare(insertQuery).bind(notAJsonString, object));
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind(notAJsonString, object));
        Row row = rows.one();
        assertRow(row, object);
    }

    @Test(groups = "short", dataProvider = "Jsr353JsonCodecTest")
    public void should_use_custom_codec_with_prepared_statements_2(JsonStructure object) throws IOException {
        session().execute(session().prepare(insertQuery).bind()
                .setString(0, notAJsonString)
                .set(1, object, JsonStructure.class)
        );
        PreparedStatement ps = session().prepare(selectQuery);
        ResultSet rows = session().execute(ps.bind()
                .setString(0, notAJsonString)
                .set(1, object, JsonStructure.class)
        );
        Row row = rows.one();
        assertRow(row, object);
    }

    private void assertRow(Row row, JsonStructure object) throws IOException {
        String json;
        StringWriter sw = new StringWriter();
        JsonWriter writer = Json.createWriter(sw);
        writer.write(object);
        json = sw.toString();
        assertThat(row.getString(0)).isEqualTo(notAJsonString);
        assertThat(row.getObject(1)).isEqualTo(json);
        assertThat(row.getString(1)).isEqualTo(json);
        assertThat(row.get(1, String.class)).isEqualTo(json);
        assertThat(row.get(1, JsonStructure.class)).isEqualTo(object);
    }

    /**
     * Validates that the {@code select().json()} capability in {@link QueryBuilder} appropriately generates a
     * 'SELECT JSON' query and that the returned {@link ResultSet} has rows with a column that is parsable using
     * {@link Jsr353JsonCodec}.
     *
     * @jira_ticket JAVA-743
     * @since 3.1.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_support_select_json_output() throws Exception {
        // when
        ResultSet r = session().execute(select().json().from(TABLE1).where(eq("k", "key0")));
        // then
        assertThat(r.getAvailableWithoutFetching()).isEqualTo(4);
        for (Row row : r) {
            JsonStructure json = row.get(0, jsonCodec);
            assertThat(json).isInstanceOf(JsonObject.class);
            JsonObject obj = (JsonObject) json;
            assertThat(obj.containsKey("k")).isTrue();
            assertThat(obj.containsKey(quote("miXeD"))).isTrue();
            assertThat(obj.containsKey("i")).isTrue();
            assertThat(obj.containsKey("f")).isTrue();

            assertThat(obj.getString("k")).isEqualTo("key0");

            int i = obj.getInt("i");
            switch (i) {
                case 1:
                    assertThat(obj.getString(quote("miXeD"))).isEqualTo("a");
                    assertThat(obj.getJsonNumber("f").doubleValue()).isEqualTo(1.1);
                    break;
                case 2:
                    assertThat(obj.getString(quote("miXeD"))).isEqualTo("b");
                    assertThat(obj.getJsonNumber("f").doubleValue()).isEqualTo(2.5);
                    break;
                case 3:
                    assertThat(obj.getString(quote("miXeD"))).isEqualTo("c");
                    assertThat(obj.getJsonNumber("f").doubleValue()).isEqualTo(3.7);
                    break;
                case 4:
                    assertThat(obj.getString(quote("miXeD"))).isEqualTo("d");
                    assertThat(obj.getJsonNumber("f").doubleValue()).isEqualTo(5.0);
                    break;
                default:
                    fail("Unexpected value for i = " + i);
            }
        }
    }

    /**
     * Validates that the {@code insertInto().json()} capability in {@link QueryBuilder} creates a query that interprets
     * the input as a json document and that fetching that data back as json returns a row with a column that matches
     * the contents of the input json document when retrieved using get with {@link JsonStructure}.
     *
     * @jira_ticket JAVA-743
     * @since 3.1.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_support_insert_json() throws Exception {
        // when
        String key = "should_support_insert_json_with_codec_format";
        JsonObject input = Json.createObjectBuilder().add("k", key)
                .add(quote("miXeD"), "miXeDValue")
                .add("f", 8.6)
                .add("i", 4)
                .build();
        session().execute(insertInto(TABLE1).json(input));
        // then
        ResultSet r = session().execute(select().json().from(TABLE1).where(eq("k", key)));
        assertThat(r.getAvailableWithoutFetching()).isEqualTo(1);

        JsonStructure output = r.one().get(0, JsonStructure.class);
        assertThat(output).isEqualTo(input);
    }

    /**
     * Validates that the {@code fromJson()} and {@code toJson} capability in {@link QueryBuilder} allows specifying
     * json input ({@code fromJson}) and retrieval of json column output ({@code toJson}).  This test inserts a row
     * with a UDT providing the UDT column value as json using {@code toJson} and then retrieves that column value
     * using {@code fromJson} and ensures the output matches the input {@link JsonStructure} for the UDT.
     *
     * @jira_ticket JAVA-743
     * @since 3.1.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_support_fromJson_and_toJson() throws Exception {
        String key = "should_support_fromJson_and_toJson";
        JsonObject inputAddr = Json.createObjectBuilder()
                .add("street", "1234 Ln.")
                .add("zipcode", 86753)
                .add("phones", Json.createArrayBuilder().add("555-5555").add("867-5309").build())
                .build();

        // insert using fromJson on address column, which is a UDT.
        session().execute(insertInto(TABLE2).value("k", key).value("v", fromJson(inputAddr)));

        // retrieve using toJson on address column.
        ResultSet r = session().execute(select().toJson("v").as("v").column("k").from(TABLE2).where(eq("k", key)));

        assertThat(r.getAvailableWithoutFetching()).isEqualTo(1);

        Row row = r.one();

        assertThat(row.getString("k")).isEqualTo(key);
        assertThat(row.get("v", JsonStructure.class)).isEqualTo(inputAddr);
    }

}
