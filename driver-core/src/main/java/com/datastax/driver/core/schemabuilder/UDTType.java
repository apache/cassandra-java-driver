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

/**
 * Represents a CQL type containing a user-defined type (UDT) in a SchemaBuilder statement.
 * <p/>
 * Use {@link SchemaBuilder#frozen(String)} or {@link SchemaBuilder#udtLiteral(String)} to build instances of this type.
 */
public final class UDTType implements ColumnType {
    private final String asCQLString;

    private UDTType(String asCQLString) {
        this.asCQLString = asCQLString;
    }

    @Override
    public String asCQLString() {
        return asCQLString;
    }

    static UDTType frozen(String udtName) {
        SchemaStatement.validateNotEmpty(udtName, "UDT name");
        return new UDTType("frozen<" + udtName + ">");
    }

    static UDTType list(UDTType elementType) {
        return new UDTType("list<" + elementType.asCQLString() + ">");
    }

    static UDTType set(UDTType elementType) {
        return new UDTType("set<" + elementType.asCQLString() + ">");
    }

    static UDTType mapWithUDTKey(UDTType keyType, DataType valueType) {
        return new UDTType("map<" + keyType.asCQLString() + ", " + valueType + ">");
    }

    static UDTType mapWithUDTValue(DataType keyType, UDTType valueType) {
        return new UDTType("map<" + keyType + ", " + valueType.asCQLString() + ">");
    }

    static UDTType mapWithUDTKeyAndValue(UDTType keyType, UDTType valueType) {
        return new UDTType("map<" + keyType.asCQLString() + ", " + valueType.asCQLString() + ">");
    }

    static UDTType literal(String literal) {
        SchemaStatement.validateNotEmpty(literal, "UDT type literal");
        return new UDTType(literal);
    }
}
