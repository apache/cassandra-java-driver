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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

// Static utilities private to the query builder
abstract class Utils {

    private static final Pattern alphanumeric = Pattern.compile("\\w+"); // this includes _
    private static final Pattern cnamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");

    /**
     * Deal with case sensitivity for a given element id (keyspace, table, column, etc.)
     *
     * This method is used to convert identifiers provided by the client (through methods such as getKeyspace(String)),
     * to the format used internally by the driver.
     *
     * We expect client-facing APIs to behave like cqlsh, that is:
     * - identifiers that are mixed-case or contain special characters should be quoted.
     * - unquoted identifiers will be lowercased: getKeyspace("Foo") will look for a keyspace named "foo"
     *
     * Copied from {@link Metadata#handleId(String)} to avoid making it public.
     */
    static String handleId(String id) {
        // Shouldn't really happen for this method, but no reason to fail here
        if (id == null)
            return null;

        if (alphanumeric.matcher(id).matches())
            return id.toLowerCase();

        // Check if it's enclosed in quotes. If it is, remove them and unescape internal double quotes
        if (!id.isEmpty() && id.charAt(0) == '"' && id.charAt(id.length() - 1) == '"')
            return id.substring(1, id.length() - 1).replaceAll("\"\"", "\"");

        // Otherwise, just return the id.
        // Note that this is a bit at odds with the rules explained above, because the client can pass an
        // identifier that contains special characters, without the need to quote it.
        // Still it's better to be lenient here rather than throwing an exception.
        return id;
    }

    static StringBuilder joinAndAppend(StringBuilder sb, CodecRegistry codecRegistry, String separator, List<? extends Appendeable> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            values.get(i).appendTo(sb, variables, codecRegistry);
        }
        return sb;
    }

    static StringBuilder joinAndAppendNames(StringBuilder sb, CodecRegistry codecRegistry, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(",");
            appendName(values.get(i), codecRegistry, sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendValues(StringBuilder sb, CodecRegistry codecRegistry, List<?> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(",");
            appendValue(values.get(i), codecRegistry, sb, variables);
        }
        return sb;
    }

    static StringBuilder appendValue(Object value, CodecRegistry codecRegistry, StringBuilder sb, List<Object> variables) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof BindMarker) {
            sb.append(value);
        } else if (value instanceof FCall) {
            FCall fcall = (FCall) value;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], codecRegistry, sb, variables);
            }
            sb.append(')');
        } else if (value instanceof Cast) {
            Cast cast = (Cast) value;
            sb.append("CAST(");
            appendName(cast.column, codecRegistry, sb);
            sb.append(" AS ").append(cast.targetType).append(")");
        } else if (value instanceof CName) {
            appendName(((CName) value).name, codecRegistry, sb);
        } else if (value instanceof RawString) {
            sb.append(value.toString());
        } else if (value instanceof List && !isSerializable(value)) {
            // bind variables are not supported inside collection literals
            appendList((List<?>) value, codecRegistry, sb);
        } else if (value instanceof Set && !isSerializable(value)) {
            // bind variables are not supported inside collection literals
            appendSet((Set<?>) value, codecRegistry, sb);
        } else if (value instanceof Map && !isSerializable(value)) {
            // bind variables are not supported inside collection literals
            appendMap((Map<?, ?>) value, codecRegistry, sb);
        } else if (variables == null || !isSerializable(value)) {
            // we are not collecting statement values (variables == null)
            // or the value is meant to be forcefully appended to the query string:
            // format it with the appropriate codec and append it now
            TypeCodec<Object> codec = codecRegistry.codecFor(value);
            sb.append(codec.format(value));
        } else {
            // Do not format the value nor append it to the query string:
            // use a bind marker instead,
            // but add the value the the statement's variables list
            sb.append('?');
            variables.add(value);
            return sb;
        }
        return sb;
    }

    private static StringBuilder appendList(List<?> l, CodecRegistry codecRegistry, StringBuilder sb) {
        sb.append('[');
        for (int i = 0; i < l.size(); i++) {
            if (i > 0)
                sb.append(',');
            appendValue(l.get(i), codecRegistry, sb, null);
        }
        sb.append(']');
        return sb;
    }

    private static StringBuilder appendSet(Set<?> s, CodecRegistry codecRegistry, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (Object elt : s) {
            if (first) first = false;
            else sb.append(',');
            appendValue(elt, codecRegistry, sb, null);
        }
        sb.append('}');
        return sb;
    }

    private static StringBuilder appendMap(Map<?, ?> m, CodecRegistry codecRegistry, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : m.entrySet()) {
            if (first)
                first = false;
            else
                sb.append(',');
            appendValue(entry.getKey(), codecRegistry, sb, null);
            sb.append(':');
            appendValue(entry.getValue(), codecRegistry, sb, null);
        }
        sb.append('}');
        return sb;
    }

    static boolean containsBindMarker(Object value) {
        if (value instanceof BindMarker)
            return true;
        if (value instanceof FCall)
            for (Object param : ((FCall) value).parameters)
                if (containsBindMarker(param))
                    return true;
        if (value instanceof Collection)
            for (Object elt : (Collection<?>) value)
                if (containsBindMarker(elt))
                    return true;
        if (value instanceof Map)
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet())
                if (containsBindMarker(entry.getKey()) || containsBindMarker(entry.getValue()))
                    return true;
        return false;
    }

    static boolean containsSpecialValue(Object value) {
        if (value instanceof BindMarker || value instanceof FCall || value instanceof CName || value instanceof RawString)
            return true;
        if (value instanceof Collection)
            for (Object elt : (Collection<?>) value)
                if (containsSpecialValue(elt))
                    return true;
        if (value instanceof Map)
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet())
                if (containsSpecialValue(entry.getKey()) || containsSpecialValue(entry.getValue()))
                    return true;
        return false;
    }

    /**
     * Return true if the given value is likely to find a suitable codec
     * to be serialized as a query parameter.
     * If the value is not serializable, it must be included in the query string.
     * Non serializable values include special values such as function calls,
     * column names and bind markers, and collections thereof.
     * We also don't serialize fixed size number types. The reason is that if we do it, we will
     * force a particular size (4 bytes for ints, ...) and for the query builder, we don't want
     * users to have to bother with that.
     *
     * @param value the value to inspect.
     * @return true if the value is serializable, false otherwise.
     */
    static boolean isSerializable(Object value) {
        if (containsSpecialValue(value))
            return false;
        if (value instanceof Number && !(value instanceof BigInteger || value instanceof BigDecimal))
            return false;
        if (value instanceof Collection)
            for (Object elt : (Collection<?>) value)
                if (!isSerializable(elt))
                    return false;
        if (value instanceof Map)
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet())
                if (!isSerializable(entry.getKey()) || !isSerializable(entry.getValue()))
                    return false;
        return true;
    }

    static boolean isIdempotent(Object value) {
        if (value == null) {
            return true;
        } else if (value instanceof Assignment) {
            Assignment assignment = (Assignment) value;
            return assignment.isIdempotent();
        } else if (value instanceof FCall) {
            return false;
        } else if (value instanceof RawString) {
            return false;
        } else if (value instanceof Collection) {
            for (Object elt : ((Collection<?>) value)) {
                if (!isIdempotent(elt))
                    return false;
            }
            return true;
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!isIdempotent(entry.getKey()) || !isIdempotent(entry.getValue()))
                    return false;
            }
        } else if (value instanceof Clause) {
            Object clauseValue = ((Clause) value).firstValue();
            return isIdempotent(clauseValue);
        }
        return true;
    }

    static StringBuilder appendName(String name, StringBuilder sb) {
        name = name.trim();
        // FIXME: checking for token( specifically is uber ugly, we'll need some better solution.
        if (name.startsWith("\"") || name.startsWith("token(") || cnamePattern.matcher(name).matches())
            sb.append(name);
        else
            sb.append('"').append(name).append('"');
        return sb;
    }

    static StringBuilder appendName(Object name, CodecRegistry codecRegistry, StringBuilder sb) {
        if (name instanceof String) {
            appendName((String) name, sb);
        } else if (name instanceof CName) {
            appendName(((CName) name).name, sb);
        } else if (name instanceof Path) {
            String[] segments = ((Path) name).segments;
            for (int i = 0; i < segments.length; i++) {
                if (i > 0)
                    sb.append('.');
                appendName(segments[i], sb);
            }
        } else if (name instanceof FCall) {
            FCall fcall = (FCall) name;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], codecRegistry, sb, null);
            }
            sb.append(')');
        } else if (name instanceof Alias) {
            Alias alias = (Alias) name;
            appendName(alias.column, codecRegistry, sb);
            sb.append(" AS ").append(alias.alias);
        } else if (name instanceof Cast) {
            Cast cast = (Cast) name;
            sb.append("CAST(");
            appendName(cast.column, codecRegistry, sb);
            sb.append(" AS ").append(cast.targetType).append(")");
        } else if (name instanceof RawString) {
            sb.append(((RawString) name).str);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return sb;
    }

    /**
     * Utility method to serialize user-provided values.
     * <p/>
     * This method is a copy of the one declared in {@link com.datastax.driver.core.SimpleStatement}, it was duplicated
     * to avoid having to make it public.
     * <p/>
     * It is useful in situations where there is no metadata available and the underlying CQL
     * type for the values is not known.
     * <p/>
     * This situation happens when a {@link com.datastax.driver.core.SimpleStatement}
     * or a {@link com.datastax.driver.core.querybuilder.BuiltStatement} (Query Builder) contain values;
     * in these places, the driver has no way to determine the right CQL type to use.
     * <p/>
     * This method performs a best-effort heuristic to guess which codec to use.
     * Note that this is not particularly efficient as the codec registry needs to iterate over
     * the registered codecs until it finds a suitable one.
     *
     * @param values          The values to convert.
     * @param protocolVersion The protocol version to use.
     * @param codecRegistry   The {@link CodecRegistry} to use.
     * @return The converted values.
     */
    static ByteBuffer[] convert(Object[] values, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                // impossible to locate the right codec when object is null,
                // so forcing the result to null
                serializedValues[i] = null;
            } else {
                if (value instanceof Token) {
                    // bypass CodecRegistry for Token instances
                    serializedValues[i] = ((Token) value).serialize(protocolVersion);
                } else {
                    try {
                        TypeCodec<Object> codec = codecRegistry.codecFor(value);
                        serializedValues[i] = codec.serialize(value, protocolVersion);
                    } catch (Exception e) {
                        // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                        throw new InvalidTypeException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, value.getClass()), e);
                    }
                }
            }
        }
        return serializedValues;
    }

    /**
     * Utility method to assemble different routing key components into a single {@link ByteBuffer}.
     * Mainly intended for statements that need to generate a routing key out of their current values.
     * <p/>
     * This method is a copy of the one declared in {@link com.datastax.driver.core.SimpleStatement}, it was duplicated
     * to avoid having to make it public.
     *
     * @param buffers the components of the routing key.
     * @return A ByteBuffer containing the serialized routing key
     */
    static ByteBuffer compose(ByteBuffer... buffers) {
        if (buffers.length == 1)
            return buffers[0];

        int totalLength = 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer buffer : buffers) {
            ByteBuffer bb = buffer.duplicate();
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

    static abstract class Appendeable {
        abstract void appendTo(StringBuilder sb, List<Object> values, CodecRegistry codecRegistry);

        abstract boolean containsBindMarker();
    }

    static class RawString {
        private final String str;

        RawString(String str) {
            this.str = str;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    static class FCall {

        private final String name;
        private final Object[] parameters;

        FCall(String name, Object... parameters) {
            checkNotNull(name);
            this.name = name;
            this.parameters = parameters;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('(');
            for (int i = 0; i < parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                sb.append(parameters[i]);
            }
            sb.append(')');
            return sb.toString();
        }

    }

    static class CName {
        private final String name;

        CName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static class Alias {
        private final Object column;
        private final String alias;

        Alias(Object column, String alias) {
            this.column = column;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return String.format("%s AS %s", column, alias);
        }
    }

    static class Cast {
        private final Object column;
        private final DataType targetType;

        Cast(Object column, DataType targetType) {
            this.column = column;
            this.targetType = targetType;
        }

        @Override
        public String toString() {
            return String.format("CAST(%s AS %s)", column, targetType);
        }
    }

    static class Path {

        private final String[] segments;

        Path(String... segments) {
            this.segments = segments;
        }

    }

}
