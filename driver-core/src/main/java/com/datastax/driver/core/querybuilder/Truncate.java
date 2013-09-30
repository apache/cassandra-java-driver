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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.TableMetadata;

/**
 * A built TRUNCATE statement.
 */
public class Truncate extends BuiltStatement {

    private final String keyspace;
    private final String table;

    Truncate(String keyspace, String table) {
        super();
        this.keyspace = keyspace;
        this.table = table;
    }

    Truncate(TableMetadata table) {
        super(table);
        this.keyspace = table.getKeyspace().getName();
        this.table = table.getName();
    }

    @Override
    protected StringBuilder buildQueryString() {
        StringBuilder builder = new StringBuilder();

        builder.append("TRUNCATE ");
        if (keyspace != null)
            Utils.appendName(keyspace, builder).append(".");
        Utils.appendName(table, builder);

        return builder;
    }
}
