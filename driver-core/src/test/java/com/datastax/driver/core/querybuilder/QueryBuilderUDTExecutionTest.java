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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@CassandraVersion("2.1.3")
public class QueryBuilderUDTExecutionTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TYPE udt (i int, a inet)",
                "CREATE TABLE udtTest(k int PRIMARY KEY, t frozen<udt>, l list<frozen<udt>>, m map<int, frozen<udt>>)");
    }

    @Test(groups = "short")
    public void insertUdtTest() throws Exception {
        UserType udtType = cluster().getMetadata().getKeyspace(keyspace).getUserType("udt");
        UDTValue udtValue = udtType.newValue().setInt("i", 2).setInet("a", InetAddress.getByName("localhost"));

        Statement insert = insertInto("udtTest").value("k", 1).value("t", udtValue);
        assertEquals(insert.toString(), "INSERT INTO udtTest (k,t) VALUES (1,{i:2,a:'127.0.0.1'});");

        session().execute(insert);

        List<Row> rows = session().execute(select().from("udtTest").where(eq("k", 1))).all();

        assertEquals(rows.size(), 1);

        Row r1 = rows.get(0);
        assertEquals("127.0.0.1", r1.getUDTValue("t").getInet("a").getHostAddress());
    }

    @Test(groups = "short")
    public void should_handle_collections_of_UDT() throws Exception {
        UserType udtType = cluster().getMetadata().getKeyspace(keyspace).getUserType("udt");
        UDTValue udtValue = udtType.newValue().setInt("i", 2).setInet("a", InetAddress.getByName("localhost"));
        UDTValue udtValue2 = udtType.newValue().setInt("i", 3).setInet("a", InetAddress.getByName("localhost"));

        Statement insert = insertInto("udtTest").value("k", 1).value("l", ImmutableList.of(udtValue));
        assertThat(insert.toString()).isEqualTo("INSERT INTO udtTest (k,l) VALUES (1,[{i:2,a:'127.0.0.1'}]);");

        session().execute(insert);

        List<Row> rows = session().execute(select().from("udtTest").where(eq("k", 1))).all();

        assertThat(rows.size()).isEqualTo(1);

        Row r1 = rows.get(0);
        assertThat(r1.getList("l", UDTValue.class).get(0).getInet("a").getHostAddress()).isEqualTo("127.0.0.1");

        Map<Integer, UDTValue> map = Maps.newHashMap();
        map.put(0, udtValue);
        map.put(2, udtValue2);
        Statement updateMap = update("udtTest").with(putAll("m", map)).where(eq("k", 1));
        assertThat(updateMap.toString())
                .isEqualTo("UPDATE udtTest SET m=m+{0:{i:2,a:'127.0.0.1'},2:{i:3,a:'127.0.0.1'}} WHERE k=1;");

        session().execute(updateMap);

        rows = session().execute(select().from("udtTest").where(eq("k", 1))).all();
        r1 = rows.get(0);
        assertThat(r1.getMap("m", Integer.class, UDTValue.class)).isEqualTo(map);
    }

    /**
     * Ensures that UDT fields can be set and retrieved on their own using {@link QueryBuilder#set} and
     * {@link QueryBuilder#select} respectively.
     *
     * @test_category queries:builder
     * @jira_ticket JAVA-1286
     * @jira_ticket CASSANDRA-7423
     */
    @CassandraVersion(value = "3.6", description = "Requires CASSANDRA-7423 introduced in Cassandra 3.6")
    @Test(groups = "short")
    public void should_support_setting_and_retrieving_udt_fields() {
        //given
        String table = "unfrozen_udt_table";
        String udt = "person";
        session().execute(createType(udt).addColumn("first", DataType.text()).addColumn("last", DataType.text()));
        UserType userType = cluster().getMetadata().getKeyspace(keyspace).getUserType(udt);
        assertThat(userType).isNotNull();

        session().execute(createTable(table).addPartitionKey("k", DataType.text())
                .addUDTColumn("u", udtLiteral(udt))
        );

        UDTValue value = userType.newValue();
        value.setString("first", "Bob");
        value.setString("last", "Smith");
        session().execute(insertInto(table).value("k", "key").value("u", value));

        //when - updating udt field
        session().execute(update(table).with(
                set(path("u", "first"), "Rick"))
                .and(set(raw("u.last"), "Jones"))
                .where(eq("k", "key")));

        //then - field should be updated and retrievable by field name.
        Row r = session().execute(select()
                .path("u", "first")
                .raw("u.last")
                .from(table)
                .where(eq("k", "key"))).one();

        assertThat(r.getString("u.first")).isEqualTo("Rick");
        assertThat(r.getString("u.last")).isEqualTo("Jones");
    }

}
