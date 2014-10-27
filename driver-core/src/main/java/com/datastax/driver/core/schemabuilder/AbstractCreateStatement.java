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

import com.datastax.driver.core.DataType;
import com.google.common.base.Optional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractCreateStatement<T extends AbstractCreateStatement<T>> extends SchemaStatement {

    protected Optional<String> keyspaceName = Optional.absent();
    protected Optional<Boolean> ifNotExists = Optional.absent();
    protected Map<String, ColumnType> simpleColumns = new LinkedHashMap<String, ColumnType>();

    protected abstract T getThis();

    /**
     * Use 'IF NOT EXISTS' CAS condition for the creation.
     *
     * @param ifNotExists whether to use the CAS condition.
     * @return this CREATE statement.
     */
    public T ifNotExists(Boolean ifNotExists) {
        this.ifNotExists = Optional.fromNullable(ifNotExists);
        return getThis();
    }

    /**
     * Adds a columnName and dataType for the custom type.
     *
     * <p>
     *     To add a list column:
     *     <pre class="code"><code class="java">
     *         addColumn("myList",DataType.list(DataType.text()))
     *     </code></pre>
     *
     *     To add a set column:
     *     <pre class="code"><code class="java">
     *         addColumn("mySet",DataType.set(DataType.text()))
     *     </code></pre>
     *
     *     To add a map column:
     *     <pre class="code"><code class="java">
     *         addColumn("myMap",DataType.map(DataType.cint(),DataType.text()))
     *     </code></pre>
     * </p>
     * @param columnName the name of the column to be added
     * @param dataType the data type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addColumn(String columnName, DataType dataType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(dataType, "Column type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, new ColumnType(dataType));
        return getThis();
    }

    /**
     * Add column with manually defined data type
     * <br/>
     * This method is useful when you want to craft yourself the data type for the column to be created.
     * <br/>
     * Examples:
     *
     * <p>
     *     To add a list column:
     *     <pre class="code"><code class="java">
     *         addColumn("myList","list&lt;text&gt;")
     *     </code></pre>
     *
     *     To add a set column:
     *     <pre class="code"><code class="java">
     *         addColumn("mySet","set&lt;text&gt;")
     *     </code></pre>
     *
     *     To add a map column:
     *     <pre class="code"><code class="java">
     *         addColumn("myMap","map&lt;int,text&gt;")
     *     </code></pre>
     *
     *     To add an UDT column:
     *     <pre class="code"><code class="java">
     *         addColumn("myUDT","frozen&lt;address&gt;")
     *     </code></pre>
     * </p>
     * @param columnName the name of the column to be added
     * @param manualDataType the text representation of the data type
     * @return
     */
    public T addColumn(String columnName, String manualDataType) {
        validateNotEmpty(columnName, "Column name");
        validateNotEmpty(manualDataType, "Manually defined data type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        simpleColumns.put(columnName, new ColumnType(manualDataType));
        return getThis();
    }

    /**
     * Adds a columnName and udt type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtType the udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTColumn(String columnName, String udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotEmpty(udtType, "UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        final StringBuilder typeBuilder = new StringBuilder(FROZEN).append(OPEN_TYPE).append(udtType).append(CLOSE_TYPE);
        simpleColumns.put(columnName, new ColumnType(typeBuilder.toString()));
        return getThis();
    }

    /**
     * Adds a columnName and list of udt type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtType the udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTListColumn(String columnName, String udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotEmpty(udtType, "UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        final StringBuilder typeBuilder = new StringBuilder(LIST)
                .append(OPEN_TYPE).append(FROZEN).append(OPEN_TYPE).append(udtType).append(CLOSE_TYPE).append(CLOSE_TYPE);
        simpleColumns.put(columnName, new ColumnType(typeBuilder.toString()));
        return getThis();
    }

    /**
     * Adds a columnName and set of udt type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtType the udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTSetColumn(String columnName, String udtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotEmpty(udtType, "UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        final StringBuilder typeBuilder = new StringBuilder(SET)
                .append(OPEN_TYPE).append(FROZEN).append(OPEN_TYPE).append(udtType).append(CLOSE_TYPE).append(CLOSE_TYPE);
        simpleColumns.put(columnName, new ColumnType(typeBuilder.toString()));
        return getThis();
    }

    /**
     * Adds a columnName and map of <raw,udt> type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param keyType the key raw type of the column to be added.
     * @param valueUdtType the value udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTMapColumn(String columnName, DataType keyType, String valueUdtType) {
        validateNotEmpty(columnName, "Column name");
        validateNotNull(keyType, "Map key type");
        validateNotEmpty(valueUdtType, "Map value UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        final StringBuilder typeBuilder = new StringBuilder(MAP)
                .append(OPEN_TYPE).append(keyType.getName().toString()).append(SEPARATOR)
                .append(FROZEN).append(OPEN_TYPE).append(valueUdtType).append(CLOSE_TYPE).append(CLOSE_TYPE);
        simpleColumns.put(columnName, new ColumnType(typeBuilder.toString()));
        return getThis();
    }

    /**
     * Adds a columnName and map of <udt,raw> type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtKeyType the key udt type of the column to be added.
     * @param valueType the value raw type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTMapColumn(String columnName, String udtKeyType, DataType valueType) {
        validateNotEmpty(columnName, "Column name");
        validateNotEmpty(udtKeyType, "Map key UDT type");
        validateNotNull(valueType, "Map valye type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        final StringBuilder typeBuilder = new StringBuilder(MAP)
                .append(OPEN_TYPE).append(FROZEN).append(OPEN_TYPE).append(udtKeyType).append(CLOSE_TYPE)
                .append(SEPARATOR).append(valueType.getName().toString()).append(CLOSE_TYPE);
        simpleColumns.put(columnName, new ColumnType(typeBuilder.toString()));
        return getThis();
    }

    /**
     * Adds a columnName and map of <udt,udt> type for the custom type.
     *
     * @param columnName the name of the column to be added
     * @param udtKeyType the key udt type of the column to be added.
     * @param udtValueType the value udt type of the column to be added.
     * @return this CREATE statement.
     *
     */
    public T addUDTMapColumn(String columnName, String udtKeyType, String udtValueType) {
        validateNotEmpty(columnName, "Column name");
        validateNotEmpty(udtKeyType, "Map key UDT type");
        validateNotEmpty(udtValueType, "Map value UDT type");
        validateNotKeyWord(columnName, String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
        final StringBuilder typeBuilder = new StringBuilder(MAP).append(OPEN_TYPE)
                .append(FROZEN).append(OPEN_TYPE).append(udtKeyType).append(CLOSE_TYPE).append(SEPARATOR)
                .append(FROZEN).append(OPEN_TYPE).append(udtValueType).append(CLOSE_TYPE).append(CLOSE_TYPE);
        simpleColumns.put(columnName, new ColumnType(typeBuilder.toString()));
        return getThis();
    }

    protected StringBuilder buildColumnType(Map.Entry<String, ColumnType> entry) {
        final ColumnType columnType = entry.getValue();
        return new StringBuilder(entry.getKey()).append(SPACE).append(columnType.buildType());

    }

    static class ColumnType {

        private final Optional<DataType> nativeType;
        private final Optional<String> rawTypeAsString;

        ColumnType(DataType nativeType) {
            this.nativeType = Optional.fromNullable(nativeType);
            this.rawTypeAsString = Optional.absent();
        }

        ColumnType(String rawTypeAsString) {
            this.nativeType = Optional.absent();
            this.rawTypeAsString = Optional.fromNullable(rawTypeAsString);
        }

        String buildType() {
            if (nativeType.isPresent()) {
                final StringBuilder column = new StringBuilder();
                final DataType dataType = nativeType.get();
                final String type = dataType.getName().toString();

                column.append(type);

                if (dataType.isCollection()) {
                    final List<DataType> typeArguments = dataType.getTypeArguments();
                    if (typeArguments.size() == 1) {
                        column.append(OPEN_TYPE).append(typeArguments.get(0)).append(CLOSE_TYPE);
                    } else if (typeArguments.size() == 2) {
                        column.append(OPEN_TYPE).append(typeArguments.get(0)).append(COMMA).append(typeArguments.get(1)).append(CLOSE_TYPE);
                    }
                }
                return column.toString();
            } else {
                return rawTypeAsString.get().toString();
            }
        }

    }
}
