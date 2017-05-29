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
package com.datastax.driver.core.schemabuilder;

import org.testng.annotations.Test;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createIndex;
import static org.assertj.core.api.Assertions.assertThat;

public class CreateIndexTest {

    @Test(groups = "unit")
    public void should_create_index() throws Exception {
        //Given //When
        SchemaStatement statement = createIndex("myIndex").ifNotExists().onTable("ks", "test").andColumn("col");

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE INDEX IF NOT EXISTS myIndex ON ks.test(col)");
    }

    @Test(groups = "unit")
    public void should_create_index_on_keys_of_map_column() throws Exception {
        //Given //When
        SchemaStatement statement = createIndex("myIndex").ifNotExists().onTable("ks", "test").andKeysOfColumn("col");

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE INDEX IF NOT EXISTS myIndex ON ks.test(KEYS(col))");
    }

}
