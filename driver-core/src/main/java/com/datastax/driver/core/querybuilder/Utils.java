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
package com.datastax.driver.core.querybuilder;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.cassandra.utils.ByteBufferUtil;

// Static utilities private to the query builder
abstract class Utils {

    private static final Pattern cnamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");

    static StringBuilder joinAndAppend(StringBuilder sb, String separator, List<? extends Appendeable> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            values.get(i).appendTo(sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendNames(StringBuilder sb, String separator, List<Object> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendName(values.get(i), sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendValues(StringBuilder sb, String separator, List<Object> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendValue(values.get(i), sb);
        }
        return sb;
    }

    static StringBuilder appendValue(Object value, StringBuilder sb) {
        return appendValue(value, sb, false);
    }

    static StringBuilder appendFlatValue(Object value, StringBuilder sb) {
        appendFlatValue(value, sb, false);
        return sb;
    }

    private static StringBuilder appendValue(Object value, StringBuilder sb, boolean rawValue) {
        // That is kind of lame but lacking a better solution
        if (appendValueIfLiteral(value, sb))
            return sb;

        if (appendValueIfCollection(value, sb, rawValue))
            return sb;

        appendStringIfValid(value, sb, rawValue);
        return sb;
    }

    private static void appendFlatValue(Object value, StringBuilder sb, boolean rawValue) {
        if (appendValueIfLiteral(value, sb))
            return;

        appendStringIfValid(value, sb, rawValue);
    }

    private static void appendStringIfValid(Object value, StringBuilder sb, boolean rawValue) {
        if (value instanceof RawString) {
            sb.append(value.toString());
        } else {
            if (!(value instanceof String)) {
                String msg = String.format("Invalid value %s of type unknown to the query builder", value);
                if (value instanceof byte[])
                    msg += " (for blob values, make sure to use a ByteBuffer)";
                throw new IllegalArgumentException(msg);
            }

            if (rawValue)
                sb.append((String)value);
            else
                appendValueString((String)value, sb);
        }
    }

    private static boolean appendValueIfLiteral(Object value, StringBuilder sb) {
        if (value instanceof Number || value instanceof UUID || value instanceof Boolean) {
            sb.append(value);
            return true;
        } else if (value instanceof InetAddress) {
            sb.append("'").append(((InetAddress)value).getHostAddress()).append("'");
            return true;
        } else if (value instanceof Date) {
            sb.append(((Date)value).getTime());
            return true;
        } else if (value instanceof ByteBuffer) {
            sb.append("0x");
            sb.append(ByteBufferUtil.bytesToHex((ByteBuffer)value));
            return true;
        } else if (value == BindMarker.ANONYMOUS) {
            sb.append("?");
            return true;
        } else if (value instanceof FCall) {
            FCall fcall = (FCall)value;
            sb.append(fcall.name).append("(");
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(",");
                appendValue(fcall.parameters[i], sb);
            }
            sb.append(")");
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

    private static boolean appendValueIfCollection(Object value, StringBuilder sb, boolean rawValue) {
        if (value instanceof List) {
            appendList((List)value, sb, rawValue);
            return true;
        } else if (value instanceof Set) {
            appendSet((Set)value, sb, rawValue);
            return true;
        } else if (value instanceof Map) {
            appendMap((Map)value, sb, rawValue);
            return true;
        } else {
            return false;
        }
    }

    static StringBuilder appendCollection(Object value, StringBuilder sb) {
        boolean wasCollection = appendValueIfCollection(value, sb, false);
        assert wasCollection;
        return sb;
    }

    static StringBuilder appendList(List<?> l, StringBuilder sb) {
        return appendList(l, sb, false);
    }

    private static StringBuilder appendList(List<?> l, StringBuilder sb, boolean rawValue) {
        sb.append("[");
        for (int i = 0; i < l.size(); i++) {
            if (i > 0)
                sb.append(",");
            appendFlatValue(l.get(i), sb, rawValue);
        }
        sb.append("]");
        return sb;
    }

    static StringBuilder appendSet(Set<?> s, StringBuilder sb) {
        return appendSet(s, sb, false);
    }

    private static StringBuilder appendSet(Set<?> s, StringBuilder sb, boolean rawValue) {
        sb.append("{");
        boolean first = true;
        for (Object elt : s) {
            if (first) first = false; else sb.append(",");
            appendFlatValue(elt, sb, rawValue);
        }
        sb.append("}");
        return sb;
    }

    static StringBuilder appendMap(Map<?, ?> m, StringBuilder sb) {
        return appendMap(m, sb, false);
    }

    private static StringBuilder appendMap(Map<?, ?> m, StringBuilder sb, boolean rawValue) {
        sb.append("{");
        boolean first = true;
        for (Map.Entry<?, ?> entry : m.entrySet()) {
            if (first)
                first = false;
            else
                sb.append(",");
            appendFlatValue(entry.getKey(), sb, rawValue);
            sb.append(":");
            appendFlatValue(entry.getValue(), sb, rawValue);
        }
        sb.append("}");
        return sb;
    }

    private static StringBuilder appendValueString(String value, StringBuilder sb) {
        return sb.append("'").append(replace(value, '\'', "''")).append("'");
    }

    static boolean isRawValue(Object value) {
        return value != null
            && !(value instanceof FCall)
            && !(value instanceof CName)
            && value != BindMarker.ANONYMOUS;
    }

    static String toRawString(Object value) {
        return appendValue(value, new StringBuilder(), true).toString();
    }

    static StringBuilder appendName(String name, StringBuilder sb) {
        name = name.trim();
        // FIXME: checking for token( specifically is uber ugly, we'll need some better solution.
        if (cnamePattern.matcher(name).matches() || name.startsWith("\"") || name.startsWith("token("))
            sb.append(name);
        else
            sb.append("\"").append(name).append("\"");
        return sb;
    }

    static StringBuilder appendName(Object name, StringBuilder sb) {
        if (name instanceof String) {
            appendName((String)name, sb);
        } else if (name instanceof CName) {
            appendName(((CName)name).name, sb);
        } else if (name instanceof FCall) {
            FCall fcall = (FCall)name;
            sb.append(fcall.name).append("(");
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(",");
                appendValue(fcall.parameters[i], sb);
            }
            sb.append(")");
        } else {
            appendName((String)name, sb);
        }
        return sb;
    }

    static abstract class Appendeable {
        abstract void appendTo(StringBuilder sb);
    }

    // Simple method to replace a single character. String.replace is a bit too
    // inefficient (see JAVA-67)
    static String replace(String text, char search, String replacement) {
        if (text == null || text.isEmpty())
            return text;

        int nbMatch = 0;
        int start = -1;
        do {
            start = text.indexOf(search, start+1);
            if (start != -1)
                ++nbMatch;
        } while (start != -1);

        if (nbMatch == 0)
            return text;

        int newLength = text.length() + nbMatch * (replacement.length() - 1);
        char[] result = new char[newLength];
        int newIdx = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == search) {
                for (int r = 0; r < replacement.length(); r++)
                    result[newIdx++] = replacement.charAt(r);
            } else {
                result[newIdx++] = c;
            }
        }
        return new String(result);
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
            sb.append(name).append("(");
            for (int i = 0; i < parameters.length; i++) {
                if (i > 0)
                    sb.append(",");
                sb.append(parameters[i]);
            }
            sb.append(")");
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
}
