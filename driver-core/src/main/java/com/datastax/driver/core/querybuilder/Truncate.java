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

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import java.util.Arrays;
import java.util.List;

/**
 * A built TRUNCATE statement.
 */
public class Truncate extends BuiltStatement {

    private final String table;

    Truncate(String keyspace, String table) {
        this(keyspace, table, null, null);
    }

    Truncate(TableMetadata table) {
        this(Metadata.quoteIfNecessary(table.getKeyspace().getName()),
                Metadata.quoteIfNecessary(table.getName()),
                Arrays.asList(new Object[table.getPartitionKey().size()]),
                table.getPartitionKey());
    }

    Truncate(String keyspace,
             String table,
             List<Object> routingKeyValues,
             List<ColumnMetadata> partitionKey) {
        super(keyspace, partitionKey, routingKeyValues);
        this.table = table;
    }

    @Override
    protected StringBuilder buildQueryString(List<Object> variables, CodecRegistry codecRegistry) {
        StringBuilder builder = new StringBuilder();

        builder.append("TRUNCATE ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append('.');
        Utils.appendName(table, builder);

        return builder;
    }
}
