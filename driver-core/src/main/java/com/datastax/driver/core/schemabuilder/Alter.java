package com.datastax.driver.core.schemabuilder;

import java.util.List;
import com.datastax.driver.core.DataType;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;

/**
 * A built ALTER TABLE statement
 */
public class Alter extends SchemaStatement {

    private Optional<String> keyspaceName = Optional.absent();
    private String tableName;

    Alter(String keyspaceName, String tableName) {
        validateNotEmpty(keyspaceName, "Keyspace name");
        validateNotEmpty(tableName, "Table name");
        this.tableName = tableName;
        this.keyspaceName = Optional.fromNullable(keyspaceName);
    }

    Alter(String tableName) {
        validateNotEmpty(tableName, "Table name");
        this.tableName = tableName;
    }

    /**
     * Alter a column
     * <p>
     *     Please note that you cannot alter a column that is part of the primary key
     * </p>
     * @param columnName the name of the column to be altered;
     * @return a new {@link com.datastax.driver.core.schemabuilder.Alter.AlterColumn} instance.
     */
    public AlterColumn alterColumn(String columnName) {
        validateNotEmpty(columnName, "Column to be altered");
        return new AlterColumn(this, columnName);
    }

    /**
     * Add a new column
     *
     * @param columnName the name of the column to be added;
     * @return a new {@link com.datastax.driver.core.schemabuilder.Alter.AddColumn} instance.
     */
    public AddColumn addColumn(String columnName) {
        validateNotEmpty(columnName, "Added column");
        return new AddColumn(this, columnName);
    }

    /**
     * Drop a column
     * <p>
     *     Please note that you cannot drop a column that is part of the primary key
     * </p>

     * @param columnName the name of the column to be dropped;
     * @return the final ALTER TABLE DROP COLUMN statement.
     */
    public String dropColumn(String columnName) {
        validateNotEmpty(columnName, "Column to be dropped");
        return new StringBuilder(this.buildInternal())
                .append(SPACE).append("DROP")
                .append(SPACE).append(columnName).toString();
    }

    /**
     * Rename a column
     * <p>
     *     Please note that you cannot rename a column that is part of the primary key
     * </p>

     * @param columnName the name of the column to be renamed;
     * @return a new {@link com.datastax.driver.core.schemabuilder.Alter.RenameColumn} instance.
     */
    public RenameColumn renameColumn(String columnName) {
        validateNotEmpty(columnName, "Column to be renamed");
        return new RenameColumn(this, columnName);
    }

    /**
     * Alter table options
     *
     * @return a new {@link com.datastax.driver.core.schemabuilder.Alter.Options} instance.
     */
    public Options withOptions() {
        return new Options(this);
    }

    /**
     * An alter column statement
     */
    public static class AlterColumn {

        private final Alter alter;
        private final String columnName;

        AlterColumn(Alter alter, String columnName) {
            this.alter = alter;
            this.columnName = columnName;
        }

        /**
         * Define the new type of the altered column
         * @param type the new type of the altered column
         * @return the final <strong>ALTER TABLE {@code columnName} TYPE {@code type} </strong> statement
         */
        public String type(DataType type) {
            final StringBuilder statement = new StringBuilder(alter.buildInternal());
            statement.append(SPACE).append("ALTER")
                    .append(SPACE).append(columnName)
                    .append(SPACE).append("TYPE")
                    .append(SPACE).append(type.toString());
            return statement.toString();
        }
    }

    /**
     * An add column statement
     */
    public static class AddColumn {

        private final Alter alter;
        private final String columnName;

        AddColumn(Alter alter, String columnName) {
            this.alter = alter;
            this.columnName = columnName;
        }

        /**
         * Define the type of the added column
         * @param type the new type of the added column
         * @return the final <strong>ALTER TABLE ADD {@code columnName} {@code type} </strong> statement
         */
        public String type(DataType type) {
            final StringBuilder statement = new StringBuilder(alter.buildInternal());
            statement.append(SPACE).append("ADD")
                    .append(SPACE).append(columnName)
                    .append(SPACE).append(type.toString());
            return statement.toString();
        }
    }

    /**
     * A rename column statement
     */
    public static class RenameColumn {

        private final Alter alter;
        private final String columnName;

        RenameColumn(Alter alter, String columnName) {
            this.alter = alter;
            this.columnName = columnName;
        }

        /**
         * Define the new name of the column
         * @param newColumnName the new name of the column*
         * @return the final <strong>ALTER TABLE RENAME {@code columnName} TO {@code newColumnName} </strong> statement
         */
        public String to(String newColumnName) {
            final StringBuilder statement = new StringBuilder(alter.buildInternal());
            validateNotEmpty(newColumnName, "New column name");
            statement.append(SPACE).append("RENAME")
                    .append(SPACE).append(columnName)
                    .append(SPACE).append("TO")
                    .append(SPACE).append(newColumnName);
            return statement.toString();
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
        String buildOptions() {
            final List<String> commonOptions = super.buildCommonOptions();
            return new StringBuilder("WITH").append(SPACE).append(Joiner.on(OPTION_SEPARATOR).join(commonOptions)).toString();
        }


        /**
         * Generate the final ALTER TABLE statement <strong>with</strong> table options
         *
         * @return the final ALTER TABLE statement <strong>with</strong> table options
         */
        @Override
        public String build() {
            return new StringBuilder(super.build()).append(SPACE).append(buildOptions()).toString();
        }

    }

    @Override
    String buildInternal() {
        StringBuilder alterStatement = new StringBuilder("ALTER TABLE");
        alterStatement.append(SPACE);
        if (keyspaceName.isPresent()) {
            alterStatement.append(keyspaceName.get()).append(DOT);
        }
        alterStatement.append(tableName);
        return alterStatement.toString();
    }
}
