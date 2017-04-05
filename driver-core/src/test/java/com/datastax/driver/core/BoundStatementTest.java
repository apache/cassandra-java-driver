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

import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.collect.Lists;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BoundStatementTest extends CCMTestsSupport {

    PreparedStatement prepared;

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE foo (k int primary key, v1 text, v2 list<int>)");
    }

    @BeforeClass(groups = "short")
    public void setup() {
        prepared = session().prepare("INSERT INTO foo (k, v1, v2) VALUES (?, ?, ?)");
    }

    @Test(groups = "short")
    public void should_get_single_value() {
        // This test is not exhaustive, note that the method is also covered in DataTypeIntegrationTest.
        BoundStatement statement = prepared.bind(1, "test", Lists.newArrayList(1));

        assertThat(statement.getInt(0))
                .isEqualTo(statement.getInt("k"))
                .isEqualTo(1);

        assertThat(statement.getString(1))
                .isEqualTo(statement.getString("v1"))
                .isEqualTo("test");

        assertThat(statement.getList(2, Integer.class))
                .isEqualTo(statement.getList("v2", Integer.class))
                .isEqualTo(Lists.newArrayList(1));

        try {
            statement.getString(0);
            fail("Expected codec not found error");
        } catch (CodecNotFoundException e) { /* expected */ }

        try {
            statement.getString(3);
            fail("Expected index error");
        } catch (IndexOutOfBoundsException e) { /* expected */ }
    }
}
