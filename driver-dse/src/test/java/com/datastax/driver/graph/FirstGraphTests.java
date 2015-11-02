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
package com.datastax.driver.graph;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.*;

public class FirstGraphTests {

    @Test(groups = "short")
    public void should_connect_to_a_cluster() throws ParseException {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        GraphSession session = new GraphSession(cluster.connect());
//        Session session = cluster.connect();
//        System.out.println("session.execute(\"select * from system.local\").one().getString(0) = " + session.execute("select * from system.local").one().getString("cluster_name"));





//        GraphStatement gs = new GraphStatement("g.V()");
//        GraphResultSet grs = session.execute("g.V()");
//        for (GraphTraversalResult gtr : grs) {
//            GraphData result = gtr.get("result");
//            System.out.println("result  = " + result);
//            System.out.println("label   = "   + result.get("label"));
//            System.out.println("age     = "     + result.get("properties").get("age").get(0).get("value"));
//            System.out.println("name    = "    + result.get("properties").get("name").get(0).get("value"));
//        }

//        SimpleStatement st = session.newSimpleStatement("g.V(x)");
//        st.setOutgoingPayload(AbstractGraphStatement.DEFAULT_GRAPH_PAYLOAD);
//
//        session.prepare(st);

        PreparedGraphStatement pgs = session.prepare("g.V(x)");

        BoundGraphStatement bgs = pgs.bind();
        bgs.setValue("x", "1");

        GraphResultSet grs = session.execute(bgs);








//        gs.setGraphKeyspace("keyspace1").setGraphLanguage("gremlin-java");
//        GraphResultSet grs = gsession.execute(gs);

//        PreparedGraphStatement pgs = gsession.prepare(new GraphStatement("select * from stresscql.typestest where lval=:x"));
//
//        BoundGraphStatement bgs = pgs.bind();
//        bgs.setValue("x", "77");
//        GraphResultSet grs = gsession.execute(bgs);
//
//        GraphTraversalResult gtr = new GraphTraversalResult("{\"result\":{\"id\":1,\"label\":\"person\",\"type\":\"vertex\",\"properties\":{\"name\":\"HENRY\",\"age\":[{\"id\":1,\"value\":29,\"properties\":{}}]}}}");
//
//        System.out.println("gtr.get(\"id\") = " + gtr.get("properties").get("age").get("id"));
//        for (GraphTraversalResult gtr : grs) {
//            System.out.println("gtr.getResultString() = " + gtr.getResultString());
//        }
//        System.out.println("grs.one().getResultString() = " + grs.one().getResultString());



//        GraphStatement gst = new GraphStatement("select * from system.local", cluster);
//        GraphResultSet grs = gsession.execute(gst);
//        System.out.println("g = " + grs.one().getResultString());
//        assertThat(grs).isNotNull();
    }
}