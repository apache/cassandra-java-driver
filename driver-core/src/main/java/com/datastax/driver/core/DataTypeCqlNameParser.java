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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.UnresolvedUserTypeException;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.ParseUtils.*;

/*
 * Parse data types from schema tables, for Cassandra 3.0 and above.
 * In these versions, data types appear as string literals, like "ascii" or "tuple<int,int>".
 *
 * Note that these methods all throw DriverInternalError when there is a parsing
 * problem because in theory we'll only parse class names coming from Cassandra and
 * so there shouldn't be anything wrong with them.
 */
class DataTypeCqlNameParser {

    private static final String FROZEN = "frozen";
    private static final String LIST = "list";
    private static final String SET = "set";
    private static final String MAP = "map";
    private static final String TUPLE = "tuple";
    private static final String EMPTY = "empty";

    private static final ImmutableMap<String, DataType> NATIVE_TYPES_MAP =
            new ImmutableMap.Builder<String, DataType>()
                    .put("ascii", ascii())
                    .put("bigint", bigint())
                    .put("blob", blob())
                    .put("boolean", cboolean())
                    .put("counter", counter())
                    .put("decimal", decimal())
                    .put("double", cdouble())
                    .put("float", cfloat())
                    .put("inet", inet())
                    .put("int", cint())
                    .put("text", text())
                    .put("varchar", varchar())
                    .put("timestamp", timestamp())
                    .put("date", date())
                    .put("time", time())
                    .put("uuid", uuid())
                    .put("varint", varint())
                    .put("timeuuid", timeuuid())
                    .put("tinyint", tinyint())
                    .put("smallint", smallint())
                    // duration is not really a native CQL type, but appears as so in system tables
                    .put("duration", duration())
                    .build();

    /**
     * @param currentUserTypes if this method gets called as part of a refresh that spans multiple user types, this contains the ones
     *                         that have already been refreshed. If the type we are parsing references a user type, we want to pick its
     *                         definition from this map in priority.
     * @param oldUserTypes     this contains all the keyspace's user types as they were before the refresh started. If we can't find a
     *                         definition in {@code currentUserTypes}, we'll check this map as a fallback.
     */
    static DataType parse(String toParse, Cluster cluster, String currentKeyspaceName, Map<String, UserType> currentUserTypes, Map<String, UserType> oldUserTypes, boolean frozen, boolean shallowUserTypes) {

        if (toParse.startsWith("'"))
            return custom(toParse.substring(1, toParse.length() - 1));

        Parser parser = new Parser(toParse, 0);
        String type = parser.parseTypeName();

        DataType nativeType = NATIVE_TYPES_MAP.get(type.toLowerCase());
        if (nativeType != null)
            return nativeType;

        if (type.equalsIgnoreCase(LIST)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 1)
                throw new DriverInternalError(String.format("Excepting single parameter for list, got %s", parameters));
            DataType elementType = parse(parameters.get(0), cluster, currentKeyspaceName, currentUserTypes, oldUserTypes, false, shallowUserTypes);
            return list(elementType, frozen);
        }

        if (type.equalsIgnoreCase(SET)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 1)
                throw new DriverInternalError(String.format("Excepting single parameter for set, got %s", parameters));
            DataType elementType = parse(parameters.get(0), cluster, currentKeyspaceName, currentUserTypes, oldUserTypes, false, shallowUserTypes);
            return set(elementType, frozen);
        }

        if (type.equalsIgnoreCase(MAP)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 2)
                throw new DriverInternalError(String.format("Excepting two parameters for map, got %s", parameters));
            DataType keyType = parse(parameters.get(0), cluster, currentKeyspaceName, currentUserTypes, oldUserTypes, false, shallowUserTypes);
            DataType valueType = parse(parameters.get(1), cluster, currentKeyspaceName, currentUserTypes, oldUserTypes, false, shallowUserTypes);
            return map(keyType, valueType, frozen);
        }

        if (type.equalsIgnoreCase(FROZEN)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 1)
                throw new DriverInternalError(String.format("Excepting single parameter for frozen keyword, got %s", parameters));
            return parse(parameters.get(0), cluster, currentKeyspaceName, currentUserTypes, oldUserTypes, true, shallowUserTypes);
        }

        if (type.equalsIgnoreCase(TUPLE)) {
            List<String> rawTypes = parser.parseTypeParameters();
            List<DataType> types = new ArrayList<DataType>(rawTypes.size());
            for (String rawType : rawTypes) {
                types.add(parse(rawType, cluster, currentKeyspaceName, currentUserTypes, oldUserTypes, false, shallowUserTypes));
            }
            return cluster.getMetadata().newTupleType(types);
        }

        // return a custom type for the special empty type
        // so that it gets detected later on, see TableMetadata
        if (type.equalsIgnoreCase(EMPTY))
            return custom(type);

        // We need to remove escaped double quotes within the type name as it is stored unescaped.
        // Otherwise it's a UDT. If we only want a shallow definition build it, otherwise search known definitions.
        if (shallowUserTypes)
            return new UserType.Shallow(currentKeyspaceName, Metadata.handleId(type), frozen);

        UserType userType = null;
        if (currentUserTypes != null)
            userType = currentUserTypes.get(Metadata.handleId(type));
        if (userType == null && oldUserTypes != null)
            userType = oldUserTypes.get(Metadata.handleId(type));

        if (userType == null)
            throw new UnresolvedUserTypeException(currentKeyspaceName, type);
        else
            return userType.copy(frozen);
    }

    private static class Parser {

        private final String str;

        private int idx;

        Parser(String str, int idx) {
            this.str = str;
            this.idx = idx;
        }

        String parseTypeName() {
            idx = skipSpaces(str, idx);
            return readNextIdentifier();
        }

        List<String> parseTypeParameters() {
            List<String> list = new ArrayList<String>();

            if (isEOS())
                return list;

            skipBlankAndComma();

            if (str.charAt(idx) != '<')
                throw new IllegalStateException();

            ++idx; // skipping '<'

            while (skipBlankAndComma()) {
                if (str.charAt(idx) == '>') {
                    ++idx;
                    return list;
                }

                try {
                    String name = parseTypeName();
                    String args = readRawTypeParameters();
                    list.add(name + args);
                } catch (DriverInternalError e) {
                    DriverInternalError ex = new DriverInternalError(String.format("Exception while parsing '%s' around char %d", str, idx));
                    ex.initCause(e);
                    throw ex;
                }
            }
            throw new DriverInternalError(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
        }

        // left idx positioned on the character stopping the read
        private String readNextIdentifier() {
            int startIdx = idx;
            if (str.charAt(startIdx) == '"') { // case-sensitive name included in double quotes
                ++idx;
                // read until closing quote.
                while (!isEOS()) {
                    boolean atQuote = str.charAt(idx) == '"';
                    ++idx;
                    if (atQuote) {
                        // if the next character is also a quote, this is an escaped
                        // quote, continue reading, otherwise stop.
                        if (!isEOS() && str.charAt(idx) == '"')
                            ++idx;
                        else
                            break;
                    }
                }
            } else if (str.charAt(startIdx) == '\'') { // custom type name included in single quotes
                ++idx;
                // read until closing quote.
                while (!isEOS() && str.charAt(idx++) != '\'') { /* loop */ }
            } else {
                while (!isEOS() && (isIdentifierChar(str.charAt(idx)) || str.charAt(idx) == '"'))
                    ++idx;
            }
            return str.substring(startIdx, idx);
        }

        // Assumes we have just read a type name and read it's potential arguments
        // blindly. I.e. it assume that either parsing is done or that we're on a '<'
        // and this reads everything up until the corresponding closing '>'. It
        // returns everything read, including the enclosing brackets.
        private String readRawTypeParameters() {
            idx = skipSpaces(str, idx);

            if (isEOS() || str.charAt(idx) == '>' || str.charAt(idx) == ',')
                return "";

            if (str.charAt(idx) != '<')
                throw new IllegalStateException(String.format("Expecting char %d of %s to be '<' but '%c' found", idx, str, str.charAt(idx)));

            int i = idx;
            int open = 1;
            boolean inQuotes = false;
            while (open > 0) {
                ++idx;

                if (isEOS())
                    throw new IllegalStateException("Non closed angle brackets");

                // Only parse for '<' and '>' characters if not within a quoted identifier.
                // Note we don't need to handle escaped quotes ("") in type names here, because they just cause inQuotes to flip
                // to false and immediately back to true
                if (!inQuotes) {
                    if (str.charAt(idx) == '"') {
                        inQuotes = true;
                    } else if (str.charAt(idx) == '<') {
                        open++;
                    } else if (str.charAt(idx) == '>') {
                        open--;
                    }
                } else if (str.charAt(idx) == '"') {
                    inQuotes = false;
                }
            }
            // we've stopped at the last closing ')' so move past that
            ++idx;
            return str.substring(i, idx);
        }

        // skip all blank and at best one comma, return true if there not EOS
        private boolean skipBlankAndComma() {
            boolean commaFound = false;
            while (!isEOS()) {
                int c = str.charAt(idx);
                if (c == ',') {
                    if (commaFound)
                        return true;
                    else
                        commaFound = true;
                } else if (!isBlank(c)) {
                    return true;
                }
                ++idx;
            }
            return false;
        }

        private boolean isEOS() {
            return idx >= str.length();
        }

        @Override
        public String toString() {
            return str.substring(0, idx) + "[" + (idx == str.length() ? "" : str.charAt(idx)) + "]" + str.substring(idx + 1);
        }
    }
}
