package com.datastax.driver.core.querybuilder;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.putAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

@CassandraVersion(major=2.1, minor=3)
public class QueryBuilderUDTExecutionTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
            return Arrays.asList("CREATE TYPE udt (i int, a inet)",
                "CREATE TABLE udtTest(k int PRIMARY KEY, t frozen<udt>, l list<frozen<udt>>, m map<int, frozen<udt>>)");
    }

    @Test(groups = "short")
    public void insertUdtTest() throws Exception {
        UserType udtType = cluster.getMetadata().getKeyspace("ks").getUserType("udt");
        UDTValue udtValue = udtType.newValue().setInt("i", 2).setInet("a", InetAddress.getByName("localhost"));

        Statement insert = insertInto("udtTest").value("k", 1).value("t", udtValue);
        assertEquals(insert.toString(), "INSERT INTO udtTest(k,t) VALUES (1,{i:2, a:'127.0.0.1'});");

        session.execute(insert);

        List<Row> rows = session.execute(select().from("udtTest").where(eq("k", 1))).all();

        assertEquals(rows.size(), 1);

        Row r1 = rows.get(0);
        assertEquals("127.0.0.1", r1.getUDTValue("t").getInet("a").getHostAddress());
    }

    @Test(groups = "short")
    public void should_handle_collections_of_UDT() throws Exception {
        UserType udtType = cluster.getMetadata().getKeyspace("ks").getUserType("udt");
        UDTValue udtValue = udtType.newValue().setInt("i", 2).setInet("a", InetAddress.getByName("localhost"));
        UDTValue udtValue2 = udtType.newValue().setInt("i", 3).setInet("a", InetAddress.getByName("localhost"));

        Statement insert = insertInto("udtTest").value("k", 1).value("l", ImmutableList.of(udtValue));
        assertThat(insert.toString()).isEqualTo("INSERT INTO udtTest(k,l) VALUES (1,[{i:2, a:'127.0.0.1'}]);");

        session.execute(insert);

        List<Row> rows = session.execute(select().from("udtTest").where(eq("k", 1))).all();

        assertThat(rows.size()).isEqualTo(1);

        Row r1 = rows.get(0);
        assertThat(r1.getList("l", UDTValue.class).get(0).getInet("a").getHostAddress()).isEqualTo("127.0.0.1");

        Map<Integer, UDTValue> map = Maps.newHashMap();
        map.put(0, udtValue);
        map.put(2, udtValue2);
        Statement updateMap = update("udtTest").with(putAll("m", map)).where(eq("k", 1));
        assertThat(updateMap.toString())
            .isEqualTo("UPDATE udtTest SET m=m+{0:{i:2, a:'127.0.0.1'},2:{i:3, a:'127.0.0.1'}} WHERE k=1;");

        session.execute(updateMap);

        rows = session.execute(select().from("udtTest").where(eq("k", 1))).all();
        r1 = rows.get(0);
        assertThat(r1.getMap("m", Integer.class, UDTValue.class)).isEqualTo(map);
    }
}
