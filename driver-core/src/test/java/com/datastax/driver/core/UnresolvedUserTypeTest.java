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
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.set;

@CassandraVersion("3.0")
public class UnresolvedUserTypeTest extends CCMTestsSupport {

    private static final String KEYSPACE = "unresolved_user_type_test";

    private static final String EXPECTED_SCHEMA = String.format("CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1' } AND DURABLE_WRITES = true;\n" +
                    "\n" +
                    "CREATE TYPE %s.g (\n" +
                    "    f1 int\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TYPE %s.h (\n" +
                    "    f1 int\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TYPE %s.\"E\" (\n" +
                    "    f1 frozen<list<frozen<%s.g>>>\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TYPE %s.\"F\" (\n" +
                    "    f1 frozen<%s.h>\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TYPE %s.\"D\" (\n" +
                    "    f1 frozen<tuple<\"F\", g, h>>\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TYPE %s.\"B\" (\n" +
                    "    f1 frozen<set<frozen<%s.\"D\">>>\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TYPE %s.\"C\" (\n" +
                    "    f1 frozen<map<frozen<%s.\"E\">, frozen<%s.\"D\">>>\n" +
                    ");\n" +
                    "\n" +
                    "CREATE TYPE %s.\"A\" (\n" +
                    "    f1 frozen<%s.\"C\">\n" +
                    ");\n", KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE,
            KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE, KEYSPACE);

    @Override
    public void onTestContextInitialized() {
        execute(
            /*
            Creates the following acyclic graph (edges directed upwards
            meaning "depends on"):

                H   G
               / \ /\
              F   |  E
               \ /  /
                D  /
               / \/
              B  C
                 |
                 A

             Topological sort order should be : gh,FE,D,CB,A
             */
                "CREATE KEYSPACE unresolved_user_type_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                String.format("CREATE TYPE %s.h (f1 int)", KEYSPACE),
                String.format("CREATE TYPE %s.g (f1 int)", KEYSPACE),
                String.format("CREATE TYPE %s.\"F\" (f1 frozen<h>)", KEYSPACE),
                String.format("CREATE TYPE %s.\"E\" (f1 frozen<list<g>>)", KEYSPACE),
                String.format("CREATE TYPE %s.\"D\" (f1 frozen<tuple<\"F\",g,h>>)", KEYSPACE),
                String.format("CREATE TYPE %s.\"C\" (f1 frozen<map<\"E\",\"D\">>)", KEYSPACE),
                String.format("CREATE TYPE %s.\"B\" (f1 frozen<set<\"D\">>)", KEYSPACE),
                String.format("CREATE TYPE %s.\"A\" (f1 frozen<\"C\">)", KEYSPACE)
        );
    }

    @Test(groups = "short")
    public void should_resolve_nested_user_types() throws ExecutionException, InterruptedException {

        // Each CREATE TYPE statement in getTableDefinitions() has triggered a partial schema refresh that
        // should have used previous UDT definitions for dependencies.
        checkUserTypes(cluster().getMetadata());

        // Create a different Cluster instance to force a full refresh where all UDTs are loaded at once.
        // The parsing logic should sort them to make sure they are loaded in the right order.
        Cluster newCluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .build());
        checkUserTypes(newCluster.getMetadata());
    }

    private void checkUserTypes(Metadata metadata) {
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(KEYSPACE);

        UserType a = keyspaceMetadata.getUserType("\"A\"");
        UserType b = keyspaceMetadata.getUserType("\"B\"");
        UserType c = keyspaceMetadata.getUserType("\"C\"");
        UserType d = keyspaceMetadata.getUserType("\"D\"");
        UserType e = keyspaceMetadata.getUserType("\"E\"");
        UserType f = keyspaceMetadata.getUserType("\"F\"");
        UserType g = keyspaceMetadata.getUserType("g");
        UserType h = keyspaceMetadata.getUserType("h");

        assertThat(a).hasField("f1", c);
        assertThat(b).hasField("f1", set(d));
        assertThat(c).hasField("f1", map(e, d));
        assertThat(d).hasField("f1", metadata.newTupleType(f, g, h));
        assertThat(e).hasField("f1", list(g));
        assertThat(f).hasField("f1", h);
        assertThat(g).hasField("f1", cint());
        assertThat(h).hasField("f1", cint());

        // JAVA-1407: ensure udts are listed in topological order
        List<UserType> userTypes = new ArrayList<UserType>(keyspaceMetadata.getUserTypes());

        assertThat(userTypes.subList(0, 2)).containsOnly(g, h);
        assertThat(userTypes.subList(2, 4)).containsOnly(e, f);
        assertThat(userTypes.subList(4, 5)).containsOnly(d);
        assertThat(userTypes.subList(5, 7)).containsOnly(b, c);
        assertThat(userTypes.subList(7, 8)).containsOnly(a);

        String script = keyspaceMetadata.exportAsString();

        // validate against a strict expectation that the schema is exactly as defined.
        assertThat(script).isEqualTo(EXPECTED_SCHEMA);
    }
}
