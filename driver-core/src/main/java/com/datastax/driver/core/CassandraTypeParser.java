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

import java.util.*;

import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.utils.Bytes;

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
    private static final String UDT_TYPE = "org.apache.cassandra.db.marshal.UserType";

    private static ImmutableMap<String, DataType> cassTypeToDataType =
        new ImmutableMap.Builder<String, DataType>()
            .put("org.apache.cassandra.db.marshal.AsciiType",         DataType.ascii())
            .put("org.apache.cassandra.db.marshal.LongType",          DataType.bigint())
            .put("org.apache.cassandra.db.marshal.BytesType",         DataType.blob())
            .put("org.apache.cassandra.db.marshal.BooleanType",       DataType.cboolean())
            .put("org.apache.cassandra.db.marshal.CounterColumnType", DataType.counter())
            .put("org.apache.cassandra.db.marshal.DecimalType",       DataType.decimal())
            .put("org.apache.cassandra.db.marshal.DoubleType",        DataType.cdouble())
            .put("org.apache.cassandra.db.marshal.FloatType",         DataType.cfloat())
            .put("org.apache.cassandra.db.marshal.InetAddressType",   DataType.inet())
            .put("org.apache.cassandra.db.marshal.Int32Type",         DataType.cint())
            .put("org.apache.cassandra.db.marshal.UTF8Type",          DataType.text())
            .put("org.apache.cassandra.db.marshal.TimestampType",     DataType.timestamp())
            .put("org.apache.cassandra.db.marshal.DateType",          DataType.timestamp())
            .put("org.apache.cassandra.db.marshal.UUIDType",          DataType.uuid())
            .put("org.apache.cassandra.db.marshal.IntegerType",       DataType.varint())
            .put("org.apache.cassandra.db.marshal.TimeUUIDType",      DataType.timeuuid())
            .build();

    static DataType parseOne(int protocolVersion, String className) {
        if (isReversed(className)) {
            // Just skip the ReversedType part, we don't care
            Parser p = new Parser(className, 0);
            p.parseNextName();
            List<String> l = p.getTypeParameters();
            if (l.size() != 1)
                throw new IllegalStateException();
            className = l.get(0);
        }

        Parser parser = new Parser(className, 0);
        String next = parser.parseNextName();

        if (next.startsWith(LIST_TYPE))
            return DataType.list(parseOne(protocolVersion, parser.getTypeParameters().get(0)));

        if (next.startsWith(SET_TYPE))
            return DataType.set(parseOne(protocolVersion, parser.getTypeParameters().get(0)));

        if (next.startsWith(MAP_TYPE)) {
            List<String> params = parser.getTypeParameters();
            return DataType.map(parseOne(protocolVersion, params.get(0)), parseOne(protocolVersion, params.get(1)));
        }

        if (isUserType(next)) {
            ++parser.idx; // skipping '('

            String keyspace = parser.readOne();
            parser.skipBlankAndComma();
            String typeName = TypeCodec.StringCodec.utf8Instance.deserialize(Bytes.fromHexString("0x" + parser.readOne()));
            parser.skipBlankAndComma();
            Map<String, String> rawFields = parser.getNameAndTypeParameters();
            List<UDTDefinition.Field> fields = new ArrayList<UDTDefinition.Field>(rawFields.size());
            for (Map.Entry<String, String> entry : rawFields.entrySet())
                fields.add(new UDTDefinition.Field(entry.getKey(), parseOne(protocolVersion, entry.getValue())));
            return DataType.userType(new UDTDefinition(protocolVersion, keyspace, typeName, fields));
        }

        DataType type = cassTypeToDataType.get(next);
        return type == null ? DataType.custom(className) : type;
    }

    public static boolean isReversed(String className) {
        return className.startsWith(REVERSED_TYPE);
    }

    public static boolean isUserType(String className) {
        return className.startsWith(UDT_TYPE);
    }

    private static boolean isComposite(String className) {
        return className.startsWith(COMPOSITE_TYPE);
    }

    private static boolean isCollection(String className) {
        return className.startsWith(COLLECTION_TYPE);
    }

    static ParseResult parseWithComposite(int protocolVersion, String className) {
        Parser parser = new Parser(className, 0);

        String next = parser.parseNextName();
        if (!isComposite(next))
            return new ParseResult(parseOne(protocolVersion, className), isReversed(next));

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
                collections.put(entry.getKey(), parseOne(protocolVersion, entry.getValue()));
        }

        List<DataType> types = new ArrayList<DataType>(count);
        List<Boolean> reversed = new ArrayList<Boolean>(count);
        for (int i = 0; i < count; i++) {
            types.add(parseOne(protocolVersion, subClassNames.get(i)));
            reversed.add(isReversed(subClassNames.get(i)));
        }

        return new ParseResult(true, types, reversed, collections);
    }

    static class ParseResult {
        public final boolean isComposite;
        public final List<DataType> types;
        public final List<Boolean> reversed;
        public final Map<String, DataType> collections;

        private ParseResult(DataType type, boolean reversed) {
            this(false,
                 Collections.<DataType>singletonList(type),
                 Collections.<Boolean>singletonList(reversed),
                 Collections.<String, DataType>emptyMap());
        }

        private ParseResult(boolean isComposite, List<DataType> types, List<Boolean> reversed, Map<String, DataType> collections) {
            this.isComposite = isComposite;
            this.types = types;
            this.reversed = reversed;
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
            if (isEOS())
                return Collections.<String, String>emptyMap();

            if (str.charAt(idx) != '(')
                throw new IllegalStateException();

            ++idx; // skipping '('

            return getNameAndTypeParameters();
        }

        // Must be at the start of the first parameter to read
        public Map<String, String> getNameAndTypeParameters() {
            // The order of the hashmap matters for UDT
            Map<String, String> map = new LinkedHashMap<String, String>();

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

        @Override
        public String toString() {
            return str.substring(0, idx) + "[" + (idx == str.length() ? "" : str.charAt(idx)) + "]" + str.substring(idx+1);
        }
    }
}
