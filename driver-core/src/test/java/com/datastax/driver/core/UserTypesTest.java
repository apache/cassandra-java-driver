/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.util.*;

import com.google.common.collect.ImmutableSet;

import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import static com.datastax.driver.core.Metadata.quote;
import static com.datastax.driver.core.TestUtils.versionCheck;

public class UserTypesTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        versionCheck(2.1, 0, "This will only work with Cassandra 2.1.0");

        String type1 = "CREATE TYPE phone (alias text, number text)";
        String type2 = "CREATE TYPE address (street text, \"ZIP\" int, phones set<phone>)";

        String table = "CREATE TABLE user (id int PRIMARY KEY, addr address)";

        return Arrays.asList(type1, type2, table);
    }

    @Test(groups = "short")
    public void simpleWriteReadTest() throws Exception {
        int userId = 0;

        try {
            PreparedStatement ins = session.prepare("INSERT INTO user(id, addr) VALUES (?, ?)");
            PreparedStatement sel = session.prepare("SELECT * FROM user WHERE id=?");

            UDTDefinition addrDef = cluster.getMetadata().getKeyspace("ks").getUserType("address");
            UDTDefinition phoneDef = cluster.getMetadata().getKeyspace("ks").getUserType("phone");

            UDTValue phone1 = phoneDef.newValue().setString("alias", "home").setString("number", "0123548790");
            UDTValue phone2 = phoneDef.newValue().setString("alias", "work").setString("number", "0698265251");

            UDTValue addr = addrDef.newValue().setString("street", "1600 Pennsylvania Ave NW").setInt(quote("ZIP"), 20500).setSet("phones", ImmutableSet.of(phone1, phone2));

            session.execute(ins.bind(userId, addr));

            Row r = session.execute(sel.bind(userId)).one();

            assertEquals(r.getInt("id"), 0);
            assertEquals(r.getUDTValue("addr"), addr);
        } catch (Exception e) {
            errorOut();
            throw e;
        }
    }
}
