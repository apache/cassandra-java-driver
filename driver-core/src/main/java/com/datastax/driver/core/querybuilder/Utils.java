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
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

// Static utilities private to the query builder
abstract class Utils {

    private static final Pattern cnamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");

    static StringBuilder joinAndAppend(StringBuilder sb, CodecRegistry codecRegistry, String separator, List<? extends Appendeable> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            values.get(i).appendTo(sb, variables, codecRegistry);
        }
        return sb;
    }

    static StringBuilder joinAndAppendNames(StringBuilder sb, CodecRegistry codecRegistry, String separator, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendName(values.get(i), codecRegistry, sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendValues(StringBuilder sb, CodecRegistry codecRegistry, String separator, List<?> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendValue(values.get(i), codecRegistry, sb, variables);
        }
        return sb;
    }

    // Returns null if it's not really serializable (function call, bind markers, ...)
    static boolean shouldInsertBindMarker(Object value) {
        if (value instanceof BindMarker || value instanceof FCall || value instanceof CName)
            return true;

        // We also don't serialize fixed size number types. The reason is that if we do it, we will
        // force a particular size (4 bytes for ints, ...) and for the query builder, we don't want
        // users to have to bother with that.
        if (value instanceof Number && !(value instanceof BigInteger || value instanceof BigDecimal))
            return true;

        return false;
    }

    static boolean containsBindMarker(Object value) {
        if (value == QueryBuilder.bindMarker())
            return true;

        if (!(value instanceof FCall))
            return false;

        FCall fcall = (FCall)value;
        for (Object param : fcall.parameters)
            if (containsBindMarker(param))
                return true;
        return false;
    }

    static StringBuilder appendValue(Object value, CodecRegistry codecRegistry, StringBuilder sb, List<Object> variables) {
        if (variables == null || !shouldInsertBindMarker(value))
            return appendValue(value, codecRegistry, sb);

        sb.append('?');
        variables.add(value);
        return sb;
    }

    static StringBuilder appendValue(Object value, CodecRegistry codecRegistry, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof BindMarker) {
            sb.append(value);
        } else if (value instanceof FCall) {
            FCall fcall = (FCall)value;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], codecRegistry, sb, null);
            }
            sb.append(')');
        } else if (value instanceof CName) {
            appendName(((CName)value).name, sb);
        } else if (value instanceof RawString) {
            sb.append(value.toString());
        } else {
            TypeCodec<Object> codec = codecRegistry.codecFor(value);
            sb.append(codec.format(value));
        }
        return sb;
    }


    static boolean isRawValue(Object value) {
        return value != null
            && !(value instanceof FCall)
            && !(value instanceof CName)
            && !(value instanceof BindMarker);
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

    static StringBuilder appendName(Object name, CodecRegistry codecRegistry, StringBuilder sb) {
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
                appendValue(fcall.parameters[i], codecRegistry, sb, null);
            }
            sb.append(')');
        } else if (name instanceof Alias) {
            Alias alias = (Alias)name;
            appendName(alias.column, codecRegistry, sb);
            sb.append(" AS ").append(alias.alias);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return sb;
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
