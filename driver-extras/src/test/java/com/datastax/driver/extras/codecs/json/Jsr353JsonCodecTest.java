/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.extras.codecs.json;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.json.*;
import java.io.IOException;
import java.io.StringWriter;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

public class Jsr353JsonCodecTest extends CCMTestsSupport {

    private static final Jsr353JsonCodec jsonCodec = new Jsr353JsonCodec();

    private static final JsonObject alice = Json.createObjectBuilder().add("id", 1).add("name", "Alice").build();
    private static final JsonObject bob = Json.createObjectBuilder().add("id", 2).add("name", "Bob").build();
    private static final JsonObject charlie = Json.createObjectBuilder().add("id", 3).add("name", "Charlie").build();

    private static final JsonArray bobAndCharlie = Json.createArrayBuilder().add(bob).add(charlie).build();

    private static final String insertQuery = "INSERT INTO t1 (c1, c2) VALUES (?, ?)";
    private static final String selectQuery = "SELECT c1, c2 FROM t1 WHERE c1 = ? and c2 = ?";

    private static final String notAJsonString = "this text is not json";

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE t1 (c1 text, c2 text, PRIMARY KEY (c1, c2))"
        );
    }

    @Override
    public Cluster.Builder createClusterBuilder() {
        return Cluster.builder().withCodecRegistry(
                new CodecRegistry().register(jsonCodec) // global User <-> varchar codec
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

}
