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

import com.google.common.base.Optional;

import java.util.Map;

/**
 * The keyspace options used in CREATE KEYSPACE or ALTER KEYSPACE statements.
 */
public class KeyspaceOptions extends SchemaStatement {

    private Optional<Map<String, Object>> replication = Optional.absent();
    private Optional<Boolean> durableWrites = Optional.absent();

    private final String command;
    private final String keyspaceName;

    public KeyspaceOptions(String command, String keyspaceName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotKeyWord(keyspaceName,
                String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));

        this.command = command;
        this.keyspaceName = keyspaceName;
    }

    /**
     * Define the replication options for the statement.
     *
     * @param replication replication properties map
     * @return this {@code KeyspaceOptions} object
     */
    public KeyspaceOptions replication(Map<String, Object> replication) {
        validateReplicationOptions(replication);
        this.replication = Optional.fromNullable(replication);
        return this;
    }

    /**
     * Define the durable writes option for the statement. If set to false,
     * data written to the keyspace will bypass the commit log.
     *
     * @param durableWrites durable write option
     * @return this {@code KeyspaceOptions} object
     */
    public KeyspaceOptions durableWrites(Boolean durableWrites) {
        this.durableWrites = Optional.fromNullable(durableWrites);
        return this;
    }

    @Override
    String buildInternal() {
        StringBuilder builtStatement = new StringBuilder(STATEMENT_START);
        builtStatement.append(command);
        builtStatement.append(" ");
        builtStatement.append(keyspaceName);
        builtStatement.append("\n\tWITH\n\t\t");

        boolean putSeparator = false;
        if (replication.isPresent()) {

            builtStatement.append("REPLICATION = {");

            int l = replication.get().entrySet().size();
            for (Map.Entry<String, Object> e : replication.get().entrySet()) {
                builtStatement.append("'")
                        .append(e.getKey())
                        .append("'")
                        .append(": ");

                if (e.getValue() instanceof String) {
                    builtStatement.append("'")
                            .append(e.getValue())
                            .append("'");
                } else {
                    builtStatement.append(e.getValue());
                }

                if (--l > 0) {
                    builtStatement.append(", ");
                }
            }

            builtStatement.append('}');
            builtStatement.append("\n\t\t");
            putSeparator = true;
        }


        if (durableWrites.isPresent()) {
            if (putSeparator) {
                builtStatement.append("AND ");
            }

            builtStatement.append("DURABLE_WRITES = " + durableWrites.get().toString());
        }


        return builtStatement.toString();
    }

    static void validateReplicationOptions(Map<String, Object> replicationOptions) {
        if (replicationOptions != null && !replicationOptions.containsKey("class")) {
            throw new IllegalArgumentException("Replication Strategy 'class' should be provided");
        }

        if (replicationOptions != null && !(replicationOptions.get("class") instanceof String)) {
            throw new IllegalArgumentException("Replication Strategy should be of type String");
        }
    }
}
