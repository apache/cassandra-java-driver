/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
package com.datastax.driver.core.schemabuilder;

import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;

public class CreateIndexTest {

    @Test
    public void should_create_index() throws Exception {
        //Given //When
        final String statement = SchemaBuilder.createIndex("myIndex").ifNotExists().onTable("ks", "test").andColumn("col");

        //Then
        assertThat(statement).isEqualTo("\n\tCREATE INDEX IF NOT EXISTS myIndex ON ks.test(col)");
    }


}
