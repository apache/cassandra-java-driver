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
package com.datastax.driver.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;

/*
 * Helps transforming CQL types (as read in the schema tables) to
 * DataTypes, starting from Cassandra 3.0.
 *
 * Note that those methods all throw DriverInternalError when there is a parsing
 * problem because in theory we'll only parse class names coming from Cassandra and
 * so there shouldn't be anything wrong with them.
 */
class DataTypeParser {

    private static final Logger logger = LoggerFactory.getLogger(DataTypeParser.class);

    private static final String FROZEN = "frozen";
    private static final String LIST = "list";
    private static final String SET = "set";
    private static final String MAP = "map";
    private static final String TUPLE = "tuple";

    private static ImmutableMap<String, DataType> nativeTypesMap =
        new ImmutableMap.Builder<String, DataType>()
            .put("ascii",     DataType.ascii())
            .put("bigint",    DataType.bigint())
            .put("blob",      DataType.blob())
            .put("boolean",   DataType.cboolean())
            .put("counter",   DataType.counter())
            .put("decimal",   DataType.decimal())
            .put("double",    DataType.cdouble())
            .put("float",     DataType.cfloat())
            .put("inet",      DataType.inet())
            .put("int",       DataType.cint())
            .put("text",      DataType.text())
            .put("varchar",   DataType.varchar())
            .put("timestamp", DataType.timestamp())
            .put("date",      DataType.date())
            .put("time",      DataType.time())
            .put("uuid",      DataType.uuid())
            .put("varint",    DataType.varint())
            .put("timeuuid",  DataType.timeuuid())
            .put("tinyint",   DataType.tinyint())
            .put("smallint",  DataType.smallint())
            .build();

    static DataType parse(String type, Metadata metadata) {
        boolean frozen = false;

        if (type.startsWith(FROZEN)) {
            frozen = true;
            type = extractNestedType(type);
        }

        Parser parser = new Parser(type, 0);
        String next = parser.parseNextName();

        if (next.startsWith(LIST))
            return DataType.list(parse(parser.getTypeParameters().get(0), metadata), frozen);

        if (next.startsWith(SET))
            return DataType.set(parse(parser.getTypeParameters().get(0), metadata), frozen);

        if (next.startsWith(MAP)) {
            List<String> params = parser.getTypeParameters();
            return DataType.map(parse(params.get(0), metadata), parse(params.get(1), metadata), frozen);
        }

        if (frozen)
            logger.warn("Got frozen keyword for something else than a collection, "
                + "this driver version might be too old for your version of Cassandra");

        if (type.startsWith(TUPLE)) {
            List<String> rawTypes = parser.getTypeParameters();
            List<DataType> types = new ArrayList<DataType>(rawTypes.size());
            for (String rawType : rawTypes) {
                types.add(parse(rawType, metadata));
            }
            return metadata.newTupleType(types);
        }

        // TODO User Defined Types

        DataType dataType = nativeTypesMap.get(next);

        if(dataType == null)
            dataType = DataType.custom(type);

        return dataType;
    }

    private static String extractNestedType(String typeName) {
        Parser p = new Parser(typeName, 0);
        p.parseNextName();
        List<String> l = p.getTypeParameters();
        if (l.size() != 1)
            throw new IllegalStateException();
        typeName = l.get(0);
        return typeName;
    }

    private static class Parser {

        private final String str;
        private int idx;

        private Parser(String str, int idx) {
            this.str = str;
            this.idx = idx;
        }

        public String parseNextName() {
            skipBlank();
            return readNextIdentifier();
        }

        public String readOne() {
            String name = parseNextName();
            String args = readRawArguments();
            return name + args;
        }

        // Assumes we have just read a type name and read it's potential arguments
        // blindly. I.e. it assume that either parsing is done or that we're on a '<'
        // and this reads everything up until the corresponding closing '>'. It
        // returns everything read, including the enclosing parenthesis.
        private String readRawArguments() {
            skipBlank();

            if (isEOS() || str.charAt(idx) == '>' || str.charAt(idx) == ',')
                return "";

            if (str.charAt(idx) != '<')
                throw new IllegalStateException(String.format("Expecting char %d of %s to be '<' but '%c' found", idx, str, str.charAt(idx)));

            int i = idx;
            int open = 1;
            while (open > 0) {
                ++idx;

                if (isEOS())
                    throw new IllegalStateException("Non closed angle brackets");

                if (str.charAt(idx) == '<') {
                    open++;
                } else if (str.charAt(idx) == '>') {
                    open--;
                }
            }
            // we've stopped at the last closing ')' so move past that
            ++idx;
            return str.substring(i, idx);
        }

        private List<String> getTypeParameters() {
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
                    list.add(readOne());
                } catch (DriverInternalError e) {
                    DriverInternalError ex = new DriverInternalError(String.format("Exception while parsing '%s' around char %d", str, idx));
                    ex.initCause(e);
                    throw ex;
                }
            }
            throw new DriverInternalError(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
        }

        private boolean isEOS() {
            return isEOS(str, idx);
        }

        private static boolean isEOS(String str, int i) {
            return i >= str.length();
        }

        private void skipBlank() {
            idx = skipBlank(str, idx);
        }

        private static int skipBlank(String str, int i) {
            while (!isEOS(str, i) && ParseUtils.isBlank(str.charAt(i)))
                ++i;

            return i;
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
                } else if (!ParseUtils.isBlank(c)) {
                    return true;
                }
                ++idx;
            }
            return false;
        }

        // left idx positioned on the character stopping the read
        public String readNextIdentifier() {
            int i = idx;
            while (!isEOS() && ParseUtils.isIdentifierChar(str.charAt(idx)))
                ++idx;

            return str.substring(i, idx);
        }

        @Override
        public String toString() {
            return str.substring(0, idx) + "[" + (idx == str.length() ? "" : str.charAt(idx)) + "]" + str.substring(idx+1);
        }
    }
}
