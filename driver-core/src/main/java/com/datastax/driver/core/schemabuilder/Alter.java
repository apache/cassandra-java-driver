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

import com.datastax.driver.core.DataType;
import com.google.common.base.Optional;

import java.util.List;

import static com.datastax.driver.core.schemabuilder.SchemaStatement.validateNotEmpty;
import static com.datastax.driver.core.schemabuilder.SchemaStatement.validateNotKeyWord;

/**
 * An in-construction ALTER TABLE statement.
 */
public class Alter implements StatementStart {

    private Optional<String> keyspaceName = Optional.absent();
    private String tableName;

    Alter(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(keyspaceName, String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
        this.keyspaceName = Optional.fromNullable(keyspaceName);
    }

    Alter(String tableName) {
        validateNotEmpty(tableName, "Table name");
        validateNotKeyWord(tableName, String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
        this.tableName = tableName;
    }

    /**
     * Add an ALTER column clause (to change the column type) to this ALTER TABLE statement.
     *
     * @param columnName the name of the column to be altered.
     * @return a new {@link Alter.AlterColumn} instance.
     */
    public AlterColumn alterColumn(String columnName) {
        validateNotEmpty(columnName, "Column to be altered");
        validateNotKeyWord(columnName, String.format("The altered column name '%s' is not allowed because it is a reserved keyword", columnName));
        return new AlterColumn(this, columnName);
    }

    /**
     * Add a new ADD column clause to this ALTER TABLE statement.
     *
     * @param columnName the name of the column to be added.
     * @return a new {@link Alter.AddColumn} instance.
     */
    public AddColumn addColumn(String columnName) {
        validateNotEmpty(columnName, "Added column");
        validateNotKeyWord(columnName, String.format("The new column name '%s' is not allowed because it is a reserved keyword", columnName));
        return new AddColumn(this, columnName, false);
    }

    /**
     * Add a new ADD column clause to this ALTER TABLE statement, to add a static column.
     *
     * @param columnName the name of the column to be added.
     * @return a new {@link Alter.AddColumn} instance.
     */
    public AddColumn addStaticColumn(String columnName) {
        validateNotEmpty(columnName, "Added static column");
        validateNotKeyWord(columnName, String.format("The new static column name '%s' is not allowed because it is a reserved keyword", columnName));
        return new AddColumn(this, columnName, true);
    }

    /**
     * Add a new DROP column clause to this ALTER TABLE statement.
     * <p/>
     * Note that you cannot drop a column that is part of the primary key.
     *
     * @param columnName the name of the column to be dropped.
     * @return the final ALTER TABLE DROP COLUMN statement.
     */
    public SchemaStatement dropColumn(String columnName) {
        validateNotEmpty(columnName, "Column to be dropped");
        validateNotKeyWord(columnName, String.format("The dropped column name '%s' is not allowed because it is a reserved keyword", columnName));
        return SchemaStatement.fromQueryString(buildInternal() + " DROP " + columnName);
    }

    /**
     * Add a new RENAME column clause to this ALTER TABLE statement.
     * <p/>
     * Note that you can only rename a column that is part of the primary key.
     *
     * @param columnName the name of the column to be renamed.
     * @return a new {@link Alter.RenameColumn} instance.
     */
    public RenameColumn renameColumn(String columnName) {
        validateNotEmpty(columnName, "Column to be renamed");
        validateNotKeyWord(columnName, String.format("The renamed column name '%s' is not allowed because it is a reserved keyword", columnName));
        return new RenameColumn(this, columnName);
    }

    /**
     * Add options (WITH clause) to this ALTER TABLE statement.
     *
     * @return a new {@link Alter.Options} instance.
     */
    public Options withOptions() {
        return new Options(this);
    }

    /**
     * An ALTER column clause.
     */
    public static class AlterColumn {

        private final Alter alter;
        private final String columnName;

        AlterColumn(Alter alter, String columnName) {
            this.alter = alter;
            this.columnName = columnName;
        }

        /**
         * Define the new type of the altered column.
         *
         * @param type the new type of the altered column.
         * @return the final statement.
         */
        public SchemaStatement type(DataType type) {
            return SchemaStatement.fromQueryString(
                    alter.buildInternal() + " ALTER " + columnName + " TYPE " + type.toString());
        }

        /**
         * Define the new type of the altered column, when that type contains a UDT.
         *
         * @param udtType the UDT type. Use {@link SchemaBuilder#frozen(String)} or {@link SchemaBuilder#udtLiteral(String)}.
         * @return the final statement.
         */
        public SchemaStatement udtType(UDTType udtType) {
            return SchemaStatement.fromQueryString(
                    alter.buildInternal() + " ALTER " + columnName + " TYPE " + udtType.asCQLString());
        }
    }

    /**
     * An ADD column clause.
     */
    public static class AddColumn {

        private final Alter alter;
        private final String columnName;
        private final boolean staticColumn;

        AddColumn(Alter alter, String columnName, boolean staticColumn) {
            this.alter = alter;
            this.columnName = columnName;
            this.staticColumn = staticColumn;
        }

        /**
         * Define the type of the added column.
         *
         * @param type the type of the added column.
         * @return the final statement.
         */
        public SchemaStatement type(DataType type) {
            return SchemaStatement.fromQueryString(
                    alter.buildInternal() + " ADD " + columnName + " " + type.toString()
                            + (staticColumn ? " static" : ""));
        }

        /**
         * Define the type of the added column, when that type contains a UDT.
         *
         * @param udtType the UDT type of the added column.
         * @return the final statement.
         */
        public SchemaStatement udtType(UDTType udtType) {
            return SchemaStatement.fromQueryString(
                    alter.buildInternal() + " ADD " + columnName + " " + udtType.asCQLString()
                            + (staticColumn ? " static" : ""));
        }
    }

    /**
     * A RENAME column clause.
     */
    public static class RenameColumn {

        private final Alter alter;
        private final String columnName;

        RenameColumn(Alter alter, String columnName) {
            this.alter = alter;
            this.columnName = columnName;
        }

        /**
         * Define the new name of the column.
         *
         * @param newColumnName the new name of the column.
         * @return the final statement.
         */
        public SchemaStatement to(String newColumnName) {
            validateNotEmpty(newColumnName, "New column name");
            validateNotKeyWord(newColumnName, String.format("The new column name '%s' is not allowed because it is a reserved keyword", newColumnName));
            return SchemaStatement.fromQueryString(
                    alter.buildInternal() + " RENAME " + columnName + " TO " + newColumnName);
        }
    }

    /**
     * The table options of an ALTER TABLE statement.
     */
    public static class Options extends TableOptions<Options> {

        Options(Alter alter) {
            super(alter);
        }

        @Override
        protected void addSpecificOptions(List<String> options) {
            // nothing to do (no specific options)
        }
    }

    @Override
    public String buildInternal() {
        String tableSpec = keyspaceName.isPresent()
                ? keyspaceName.get() + "." + tableName
                : tableName;

        return SchemaStatement.STATEMENT_START + "ALTER TABLE " + tableSpec;
    }
}
