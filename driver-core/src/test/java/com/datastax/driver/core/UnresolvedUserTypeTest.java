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
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.*;

@CassandraVersion(major = 3.0)
public class UnresolvedUserTypeTest extends CCMTestsSupport {

    @Override
    public Collection<String> createTestFixtures() {
        return Lists.newArrayList(
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

             Topological sort order should be : GH,FE,D,CB,A
             */
                String.format("CREATE TYPE %s.h (f1 int)", keyspace),
                String.format("CREATE TYPE %s.g (f1 int)", keyspace),
                String.format("CREATE TYPE %s.\"F\" (f1 frozen<h>)", keyspace),
                String.format("CREATE TYPE %s.\"E\" (f1 frozen<list<g>>)", keyspace),
                String.format("CREATE TYPE %s.\"D\" (f1 frozen<tuple<\"F\",g,h>>)", keyspace),
                String.format("CREATE TYPE %s.\"C\" (f1 frozen<map<\"E\",\"D\">>)", keyspace),
                String.format("CREATE TYPE %s.\"B\" (f1 frozen<set<\"D\">>)", keyspace),
                String.format("CREATE TYPE %s.\"A\" (f1 frozen<\"C\">)", keyspace)
        );
    }

    @Test(groups = "short")
    public void should_resolve_nested_user_types() throws ExecutionException, InterruptedException {

        // Each CREATE TYPE statement in getTableDefinitions() has triggered a partial schema refresh that
        // should have used previous UDT definitions for dependencies.
        checkUserTypes(cluster.getMetadata());

        // Create a different Cluster instance to force a full refresh where all UDTs are loaded at once.
        // The parsing logic should sort them to make sure they are loaded in the right order.
        Cluster newCluster = register(Cluster.builder()
                .addContactPointsWithPorts(getInitialContactPoints())
                .withPort(ccm.getBinaryPort())
                .build());
        checkUserTypes(newCluster.getMetadata());
    }

    private void checkUserTypes(Metadata metadata) {
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);

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
    }
}
