/*
 * Copyright DataStax, Inc.
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

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.alterKeyspace;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class AlterKeyspaceTest {

  @Test(groups = "unit")
  public void should_alter_keyspace_with_options() throws Exception {
    Map<String, Object> replicationOptions = new HashMap<String, Object>();
    replicationOptions.put("class", "SimpleStrategy");
    replicationOptions.put("replication_factor", 1);

    // When
    SchemaStatement statement =
        alterKeyspace("test").with().durableWrites(true).replication(replicationOptions);

    // Then
    assertThat(statement.getQueryString())
        .isEqualTo(
            "\n\tALTER KEYSPACE test"
                + "\n\tWITH\n\t\t"
                + "REPLICATION = {'replication_factor': 1, 'class': 'SimpleStrategy'}\n\t\t"
                + "AND DURABLE_WRITES = true");
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void incorrect_replication_options() throws Exception {
    Map<String, Object> replicationOptions = new HashMap<String, Object>();
    replicationOptions.put("class", 5);

    // When
    alterKeyspace("test").with().replication(replicationOptions);
  }
}
