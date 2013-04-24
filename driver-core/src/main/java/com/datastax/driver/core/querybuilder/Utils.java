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
    private static final Pattern fctsPattern = Pattern.compile("\\s*[a-zA-Z]\\w*\\(.+");

    static StringBuilder joinAndAppend(StringBuilder sb, String separator, List<? extends Appendeable> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            values.get(i).appendTo(sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendNames(StringBuilder sb, String separator, List<String> values) {
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

        if (rawValue || isFunctionCall(value) || value instanceof RawString)
            return sb.append(value.toString());
        else
            return appendValueString(value.toString(), sb);
    }

    static boolean isFunctionCall(Object value) {
        return value instanceof String && fctsPattern.matcher((String)value).matches();
    }

    private static void appendFlatValue(Object value, StringBuilder sb, boolean rawValue) {
        if (appendValueIfLiteral(value, sb))
            return;

        if (rawValue || isFunctionCall(value) || value instanceof RawString)
            sb.append(value.toString());
        else
            appendValueString(value.toString(), sb);
    }

    private static boolean appendValueIfLiteral(Object value, StringBuilder sb) {
        if (value instanceof Number || value instanceof UUID || value instanceof Boolean) {
            sb.append(value);
            return true;
        } else if (value instanceof InetAddress) {
            sb.append(((InetAddress)value).getHostAddress());
            return true;
        } else if (value instanceof Date) {
            sb.append(((Date)value).getTime());
            return true;
        } else if (value instanceof ByteBuffer) {
            sb.append("0x");
            sb.append(ByteBufferUtil.bytesToHex((ByteBuffer)value));
            return true;
        } else if (value == QueryBuilder.BIND_MARKER) {
            sb.append("?");
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

    static String toRawString(Object value) {
        return appendValue(value, new StringBuilder(), true).toString();
    }

    static StringBuilder appendName(String name, StringBuilder sb) {
        name = name.trim();
        if (cnamePattern.matcher(name).matches() || name.startsWith("\"") || fctsPattern.matcher(name).matches())
            sb.append(name);
        else
            sb.append("\"").append(name).append("\"");
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
            return "'" + str + "'";
        }
    }
}
