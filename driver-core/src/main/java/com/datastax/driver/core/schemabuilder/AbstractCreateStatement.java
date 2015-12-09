/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.datastax.driver.core.DataType;
import com.google.common.base.Optional;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractCreateStatement<T extends AbstractCreateStatement<T>> extends SchemaStatement {

    protected Optional<String> keyspaceName = Optional.absent();
    protected boolean ifNotExists;
    protected Map<String, ColumnType> simpleColumns = new LinkedHashMap<String, ColumnType>();

    @SuppressWarnings("unchecked")
    private T self = (T) this;

    /**
     * Add the 'IF NOT EXISTS' condition to this CREATE statement.
     *
     * @return this CREATE statement.
     */
    public T ifNotExists() {
        this.ifNotExists = true;
        return self;
    }

    /**
     * Add a column definition to this CREATE statement.
     * <p/>
     * <p/>
     * To add a list column:
     * <pre class="code"><code class="java">
     * addColumn("myList",DataType.list(DataType.text()))
     * </code></pre>
     * <p/>
     * To add a set column:
     * <pre class="code"><code class="java">
     * addColumn("mySet",DataType.set(DataType.text()))
     * </code></pre>
     * <p/>
     * To add a map column:
     * <pre class="code"><code class="java">
     * addColumn("myMap",DataType.map(DataType.cint(),DataType.text()))
     * </code></pre>
     *
     * @param columnName the name of the column to be added.
     * @param dataType   the data type of the column to be added.
     * @return this CREATE statement.
     */
    public T addColumn(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(dataType, "Column type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, new NativeColumnType(dataType));
        return self;
    }

    /**
     * Add a column definition to this CREATE statement, when the type contains a UDT.
     *
     * @param columnName the name of the column to be added.
     * @param udtType    the UDT type of the column to be added. Use {@link SchemaBuilder#frozen(String)} or {@link SchemaBuilder#udtLiteral(String)}.
     * @return this CREATE statement.
     */
    public T addUDTColumn(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtType, "Column type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, udtType);
        return self;
    }

    /**
     * Shorthand to add a column definition to this CREATE statement, when the type is a list of UDT.
     *
     * @param columnName the name of the column to be added
     * @param udtType    the udt type of the column to be added. Use {@link SchemaBuilder#frozen(String)}.
     * @return this CREATE statement.
     */
    public T addUDTListColumn(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtType, "Column element type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.list(udtType));
        return self;
    }

    /**
     * Shorthand to add a column definition to this CREATE statement, when the type is a set of UDT.
     *
     * @param columnName the name of the column to be added
     * @param udtType    the udt type of the column to be added. Use {@link SchemaBuilder#frozen(String)}.
     * @return this CREATE statement.
     */
    public T addUDTSetColumn(String columnName, UDTType udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtType, "Column element type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.set(udtType));
        return self;
    }

    /**
     * Shorthand to add a column definition to this CREATE statement, when the type is a map with a UDT value type.
     * <p/>
     * Example:
     * <pre>
     *     addUDTMapColumn("addresses", DataType.text(), frozen("address"));
     * </pre>
     *
     * @param columnName   the name of the column to be added.
     * @param keyType      the key type of the column to be added.
     * @param valueUdtType the value UDT type of the column to be added. Use {@link SchemaBuilder#frozen(String)}.
     * @return this CREATE statement.
     */
    public T addUDTMapColumn(String columnName, DataType keyType, UDTType valueUdtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(keyType, "Map key type");
        validateNotNull(valueUdtType, "Map value UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.mapWithUDTValue(keyType, valueUdtType));
        return self;
    }

    /**
     * Shorthand to add a column definition to this CREATE statement, when the type is a map with a UDT key type.
     * <p/>
     * Example:
     * <pre>
     *     addUDTMapColumn("roles", frozen("user"), DataType.text());
     * </pre>
     *
     * @param columnName the name of the column to be added.
     * @param udtKeyType the key UDT type of the column to be added. Use {@link SchemaBuilder#frozen(String)}.
     * @param valueType  the value raw type of the column to be added.
     * @return this CREATE statement.
     */
    public T addUDTMapColumn(String columnName, UDTType udtKeyType, DataType valueType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtKeyType, "Map key UDT type");
        validateNotNull(valueType, "Map value type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.mapWithUDTKey(udtKeyType, valueType));
        return self;
    }

    /**
     * Shorthand to add a column definition to this CREATE statement, when the type is a map with UDT key and value types.
     * <p/>
     * Example:
     * <pre>
     *     addUDTMapColumn("users", frozen("user"), frozen("address"));
     * </pre>
     *
     * @param columnName   the name of the column to be added.
     * @param udtKeyType   the key UDT type of the column to be added. Use {@link SchemaBuilder#frozen(String)}.
     * @param udtValueType the value UDT type of the column to be added. Use {@link SchemaBuilder#frozen(String)}.
     * @return this CREATE statement.
     */
    public T addUDTMapColumn(String columnName, UDTType udtKeyType, UDTType udtValueType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(udtKeyType, "Map key UDT type");
        validateNotNull(udtValueType, "Map value UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, UDTType.mapWithUDTKeyAndValue(udtKeyType, udtValueType));
        return self;
    }

    protected String buildColumnType(Map.Entry<String, ColumnType> entry) {
        final ColumnType columnType = entry.getValue();
        return entry.getKey() + " " + columnType.asCQLString();
    }
}
