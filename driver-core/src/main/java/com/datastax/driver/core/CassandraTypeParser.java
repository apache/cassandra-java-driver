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
package com.datastax.driver.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableMap;

/*
 * Helps transforming Cassandra types (as read in the schema tables) to
 * DataType.
 *
 * This is modified (and simplified) from Cassandra's TypeParser class to suit
 * our needs. In particular it's not very efficient, but it doesn't really matter
 * since it's rarely used and never in a critical path.
 *
 * Note that those methods all throw DriverInternalError when there is a parsing
 * problem because in theory we'll only parse class names coming from Cassandra and
 * so there shouldn't be anything wrong with them.
 */
class CassandraTypeParser {

    private static final String REVERSED_TYPE = "org.apache.cassandra.db.marshal.ReversedType";
    private static final String COMPOSITE_TYPE = "org.apache.cassandra.db.marshal.CompositeType";
    private static final String COLLECTION_TYPE = "org.apache.cassandra.db.marshal.ColumnToCollectionType";
    private static final String LIST_TYPE = "org.apache.cassandra.db.marshal.ListType";
    private static final String SET_TYPE = "org.apache.cassandra.db.marshal.SetType";
    private static final String MAP_TYPE = "org.apache.cassandra.db.marshal.MapType";

    private static ImmutableMap<String, DataType> cassTypeToDataType =
        new ImmutableMap.Builder<String, DataType>()
            .put("org.apache.cassandra.db.marshal.AsciiType",       DataType.ascii())
            .put("org.apache.cassandra.db.marshal.LongType",        DataType.bigint())
            .put("org.apache.cassandra.db.marshal.BlobType",        DataType.blob())
            .put("org.apache.cassandra.db.marshal.BooleanType",     DataType.cboolean())
            .put("org.apache.cassandra.db.marshal.CounterType",     DataType.counter())
            .put("org.apache.cassandra.db.marshal.DecimalType",     DataType.decimal())
            .put("org.apache.cassandra.db.marshal.DoubleType",      DataType.cdouble())
            .put("org.apache.cassandra.db.marshal.FloatType",       DataType.cfloat())
            .put("org.apache.cassandra.db.marshal.InetAddressType", DataType.inet())
            .put("org.apache.cassandra.db.marshal.Int32Type",       DataType.cint())
            .put("org.apache.cassandra.db.marshal.UTF8Type",        DataType.text())
            .put("org.apache.cassandra.db.marshal.TimestampType",   DataType.timestamp())
            .put("org.apache.cassandra.db.marshal.DateType",        DataType.timestamp())
            .put("org.apache.cassandra.db.marshal.UUIDType",        DataType.uuid())
            .put("org.apache.cassandra.db.marshal.IntegerType",     DataType.varint())
            .put("org.apache.cassandra.db.marshal.TimeUUIDType",    DataType.timeuuid())
            .build();

    static DataType parseOne(String className) {
        Parser parser = new Parser(className, 0);
        String next = parser.parseNextName();

        if (isReversed(next)) {
            List<String> l = parser.getTypeParameters();
            if (l.size() != 1)
                throw new IllegalStateException();
            className = l.get(0);
        }

        if (className.startsWith(LIST_TYPE)) {
            parser.parseNextName();
            List<String> params = parser.getTypeParameters();
            return DataType.list(parseOne(params.get(0)));
        } else if (className.startsWith(SET_TYPE)) {
            parser.parseNextName();
            List<String> params = parser.getTypeParameters();
            return DataType.set(parseOne(params.get(0)));
        } else if (className.startsWith(MAP_TYPE)) {
            parser.parseNextName();
            List<String> params = parser.getTypeParameters();
            return DataType.map(parseOne(params.get(0)), parseOne(params.get(1)));
        }

        DataType type = cassTypeToDataType.get(className);
        return type == null ? DataType.custom(className) : type;
    }

    public static boolean isReversed(String className) {
        return className.startsWith(REVERSED_TYPE);
    }

    private static boolean isComposite(String className) {
        return className.startsWith(COMPOSITE_TYPE);
    }

    private static boolean isCollection(String className) {
        return className.startsWith(COLLECTION_TYPE);
    }

    static ParseResult parseWithComposite(String className) {
        Parser parser = new Parser(className, 0);

        String next = parser.parseNextName();
        if (!isComposite(next))
            return new ParseResult(parseOne(className));

        List<String> subClassNames = parser.getTypeParameters();
        int count = subClassNames.size();
        String last = subClassNames.get(count - 1);
        Map<String, DataType> collections = new HashMap<String, DataType>();
        if (isCollection(last)) {
            count--;
            Parser collectionParser = new Parser(last, 0);
            collectionParser.parseNextName(); // skips columnToCollectionType
            Map<String, String> params = collectionParser.getCollectionsParameters();
            for (Map.Entry<String, String> entry : params.entrySet())
                collections.put(entry.getKey(), parseOne(entry.getValue()));
        }

        List<DataType> types = new ArrayList<DataType>(count);
        for (int i = 0; i < count; i++)
            types.add(parseOne(subClassNames.get(i)));

        return new ParseResult(true, types, collections);
    }

    static class ParseResult {
        public final boolean isComposite;
        public final List<DataType> types;
        public final Map<String, DataType> collections;

        private ParseResult(DataType type) {
            this(false, Collections.<DataType>singletonList(type), Collections.<String, DataType>emptyMap());
        }

        private ParseResult(boolean isComposite, List<DataType> types, Map<String, DataType> collections) {
            this.isComposite = isComposite;
            this.types = types;
            this.collections = collections;
        }
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

        // Assumes we have just read a class name and read it's potential arguments
        // blindly. I.e. it assume that either parsing is done or that we're on a '('
        // and this reads everything up until the corresponding closing ')'. It
        // returns everything read, including the enclosing parenthesis.
        private String readRawArguments() {
            skipBlank();

            if (isEOS() || str.charAt(idx) == ')' || str.charAt(idx) == ',')
                return "";

            if (str.charAt(idx) != '(')
                throw new IllegalStateException(String.format("Expecting char %d of %s to be '(' but '%c' found", idx, str, str.charAt(idx)));

            int i = idx;
            int open = 1;
            while (open > 0) {
                ++idx;

                if (isEOS())
                    throw new IllegalStateException("Non closed parenthesis");

                if (str.charAt(idx) == '(') {
                    open++;
                } else if (str.charAt(idx) == ')') {
                    open--;
                }
            }
            // we've stopped at the last closing ')' so move past that
            ++idx;
            return str.substring(i, idx);
        }

        public List<String> getTypeParameters() {
            List<String> list = new ArrayList<String>();

            if (isEOS())
                return list;

            if (str.charAt(idx) != '(')
                throw new IllegalStateException();

            ++idx; // skipping '('

            while (skipBlankAndComma()) {
                if (str.charAt(idx) == ')') {
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

        public Map<String, String> getCollectionsParameters() {
            Map<String, String> map = new HashMap<String, String>();

            if (isEOS())
                return map;

            if (str.charAt(idx) != '(')
                throw new IllegalStateException();

            ++idx; // skipping '('

            while (skipBlankAndComma()) {
                if (str.charAt(idx) == ')') {
                    ++idx;
                    return map;
                }

                String bbHex = readNextIdentifier();
                String name = null;
                try {
                    name = TypeCodec.StringCodec.utf8Instance.deserialize(Bytes.fromHexString("0x" + bbHex));
                } catch (NumberFormatException e) {
                    throwSyntaxError(e.getMessage());
                }

                skipBlank();
                if (str.charAt(idx) != ':')
                    throwSyntaxError("expecting ':' token");

                ++idx;
                skipBlank();
                try {
                    map.put(name, readOne());
                } catch (DriverInternalError e) {
                    DriverInternalError ex = new DriverInternalError(String.format("Exception while parsing '%s' around char %d", str, idx));
                    ex.initCause(e);
                    throw ex;
                }
            }
            throw new DriverInternalError(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
        }

        private void throwSyntaxError(String msg) {
            throw new DriverInternalError(String.format("Syntax error parsing '%s' at char %d: %s", str, idx, msg));
        }

        private boolean isEOS() {
            return isEOS(str, idx);
        }

        private static boolean isEOS(String str, int i) {
            return i >= str.length();
        }

        private static boolean isBlank(int c) {
            return c == ' ' || c == '\t' || c == '\n';
        }

        private void skipBlank() {
            idx = skipBlank(str, idx);
        }

        private static int skipBlank(String str, int i) {
            while (!isEOS(str, i) && isBlank(str.charAt(i)))
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
                } else if (!isBlank(c)) {
                    return true;
                }
                ++idx;
            }
            return false;
        }

        /*
         * [0..9a..bA..B-+._&]
         */
        private static boolean isIdentifierChar(int c) {
            return (c >= '0' && c <= '9')
                || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                || c == '-' || c == '+' || c == '.' || c == '_' || c == '&';
        }

        // left idx positioned on the character stopping the read
        public String readNextIdentifier() {
            int i = idx;
            while (!isEOS() && isIdentifierChar(str.charAt(idx)))
                ++idx;

            return str.substring(i, idx);
        }

        public char readNextChar() {
            skipBlank();
            return str.charAt(idx++);
        }
    }
}
