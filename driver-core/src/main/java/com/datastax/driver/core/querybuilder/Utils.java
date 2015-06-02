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
package com.datastax.driver.core.querybuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;

// Static utilities private to the query builder
abstract class Utils {

    private static final Pattern cnamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");

    static StringBuilder joinAndAppend(StringBuilder sb, String separator, List<? extends Appendeable> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            values.get(i).appendTo(sb, variables);
        }
        return sb;
    }

    static StringBuilder joinAndAppendNames(StringBuilder sb, String separator, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendName(values.get(i), sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendValues(StringBuilder sb, String separator, List<?> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendValue(values.get(i), sb, variables);
        }
        return sb;
    }

    // Returns null if it's not really serializable (function call, bind markers, ...)
    static boolean isSerializable(Object value) {
        if (value instanceof BindMarker || value instanceof FCall || value instanceof CName)
            return false;

        // We also don't serialize fixed size number types. The reason is that if we do it, we will
        // force a particular size (4 bytes for ints, ...) and for the query builder, we don't want
        // users to have to bother with that.
        if (value instanceof Number && !(value instanceof BigInteger || value instanceof BigDecimal))
            return false;

        return true;
    }

    static ByteBuffer[] convert(List<Object> values, ProtocolVersion protocolVersion) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.size()];
        for (int i = 0; i < values.size(); i++) {
            try {
                serializedValues[i] = DataType.serializeValue(values.get(i), protocolVersion);
            } catch (IllegalArgumentException e) {
                // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                throw new IllegalArgumentException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, values.get(i).getClass()));
            }
        }
        return serializedValues;
    }

    static StringBuilder appendValue(Object value, StringBuilder sb, List<Object> variables) {
        if (variables == null || !isSerializable(value))
            return appendValue(value, sb);

        sb.append('?');
        variables.add(value);
        return sb;
    }

    static StringBuilder appendValue(Object value, StringBuilder sb) {
        // That is kind of lame but lacking a better solution
        if (appendValueIfLiteral(value, sb))
            return sb;

        if (appendValueIfCollection(value, sb))
            return sb;

        if (appendValueIfUdt(value, sb))
            return sb;

        if (appendValueIfTuple(value, sb))
            return sb;

        appendStringIfValid(value, sb);
        return sb;
    }

    private static void appendStringIfValid(Object value, StringBuilder sb) {
        if (value instanceof RawString) {
            sb.append(value.toString());
        } else {
            if (!(value instanceof String)) {
                String msg = String.format("Invalid value %s of type unknown to the query builder", value);
                if (value instanceof byte[])
                    msg += " (for blob values, make sure to use a ByteBuffer)";
                throw new IllegalArgumentException(msg);
            }
            appendValueString((String)value, sb);
        }
    }

    private static boolean appendValueIfLiteral(Object value, StringBuilder sb) {
        if (value instanceof Number || value instanceof UUID || value instanceof Boolean) {
            sb.append(value);
            return true;
        } else if (value instanceof InetAddress) {
            sb.append(DataType.inet().format(value));
            return true;
        } else if (value instanceof Date) {
            sb.append(DataType.timestamp().format(value));
            return true;
        } else if (value instanceof ByteBuffer) {
            sb.append(DataType.blob().format(value));
            return true;
        } else if (value instanceof BindMarker) {
            sb.append(value);
            return true;
        } else if (value instanceof FCall) {
            FCall fcall = (FCall)value;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], sb, null);
            }
            sb.append(')');
            return true;
        } else if (value instanceof CName) {
            appendName(((CName)value).name, sb);
            return true;
        } else if (value == null) {
            sb.append("null");
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("rawtypes")
    private static boolean appendValueIfCollection(Object value, StringBuilder sb) {
        if (value instanceof List) {
            appendList((List)value, sb);
            return true;
        } else if (value instanceof Set) {
            appendSet((Set)value, sb);
            return true;
        } else if (value instanceof Map) {
            appendMap((Map)value, sb);
            return true;
        } else {
            return false;
        }
    }

    static StringBuilder appendCollection(Object value, StringBuilder sb, List<Object> variables) {
        if (variables == null || !isSerializable(value)) {
            boolean wasCollection = appendValueIfCollection(value, sb);
            assert wasCollection;
        } else {
            sb.append('?');
            variables.add(value);
        }
        return sb;
    }

    static StringBuilder appendList(List<?> l, StringBuilder sb) {
        sb.append('[');
        for (int i = 0; i < l.size(); i++) {
            if (i > 0)
                sb.append(',');
            appendValue(l.get(i), sb);
        }
        sb.append(']');
        return sb;
    }

    static StringBuilder appendSet(Set<?> s, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (Object elt : s) {
            if (first) first = false; else sb.append(',');
            appendValue(elt, sb);
        }
        sb.append('}');
        return sb;
    }

    static StringBuilder appendMap(Map<?, ?> m, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : m.entrySet()) {
            if (first)
                first = false;
            else
                sb.append(',');
            appendValue(entry.getKey(), sb);
            sb.append(':');
            appendValue(entry.getValue(), sb);
        }
        sb.append('}');
        return sb;
    }

    private static boolean appendValueIfUdt(Object value, StringBuilder sb) {
        if (value instanceof UDTValue) {
            sb.append(((UDTValue)value).toString());
            return true;
        } else {
            return false;
        }
    }

    private static boolean appendValueIfTuple(Object value, StringBuilder sb) {
        if (value instanceof TupleValue) {
            sb.append(((TupleValue)value).toString());
            return true;
        } else {
            return false;
        }
    }

    static boolean containsBindMarker(Object value) {
        if (value instanceof BindMarker)
            return true;

        if (!(value instanceof FCall))
            return false;

        FCall fcall = (FCall)value;
        for (Object param : fcall.parameters)
            if (containsBindMarker(param))
                return true;
        return false;
    }

    private static StringBuilder appendValueString(String value, StringBuilder sb) {
        return sb.append(DataType.text().format(value));
    }

    static boolean isRawValue(Object value) {
        return value != null
            && !(value instanceof FCall)
            && !(value instanceof CName)
            && !(value instanceof BindMarker);
    }

    static String toRawString(Object value) {
        return appendValue(value, new StringBuilder()).toString();
    }

    static StringBuilder appendName(String name, StringBuilder sb) {
        name = name.trim();
        // FIXME: checking for token( specifically is uber ugly, we'll need some better solution.
        if (cnamePattern.matcher(name).matches() || name.startsWith("\"") || name.startsWith("token("))
            sb.append(name);
        else
            sb.append('"').append(name).append('"');
        return sb;
    }

    static StringBuilder appendName(Object name, StringBuilder sb) {
        if (name instanceof String) {
            appendName((String)name, sb);
        } else if (name instanceof CName) {
            appendName(((CName)name).name, sb);
        } else if (name instanceof FCall) {
            FCall fcall = (FCall)name;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], sb, null);
            }
            sb.append(')');
        } else if (name instanceof Alias) {
            Alias alias = (Alias)name;
            appendName(alias.column, sb);
            sb.append(" AS ").append(alias.alias);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return sb;
    }

    static abstract class Appendeable {
        abstract void appendTo(StringBuilder sb, List<Object> values);
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
}
