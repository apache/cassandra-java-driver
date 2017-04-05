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

import com.datastax.driver.core.CodecRegistry;

import java.util.List;

import static com.datastax.driver.core.querybuilder.Utils.appendName;
import static com.datastax.driver.core.querybuilder.Utils.appendValue;

public abstract class Assignment extends Utils.Appendeable {

    final Object name;

    private Assignment(Object name) {
        this.name = name;
    }

    /**
     * The name of the column this assignment applies to.
     *
     * @return the name of the column this assignment applies to.
     */
    public String getColumnName() {
        return name.toString();
    }

    abstract boolean isIdempotent();

    static class SetAssignment extends Assignment {

        private final Object value;

        SetAssignment(Object name, Object value) {
            super(name);
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            appendName(name, codecRegistry, sb);
            sb.append('=');
            appendValue(value, codecRegistry, sb, variables);
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }

        @Override
        boolean isIdempotent() {
            return Utils.isIdempotent(value);
        }
    }

    static class CounterAssignment extends Assignment {

        private final Object value;
        private final boolean isIncr;

        CounterAssignment(String name, Object value, boolean isIncr) {
            super(name);
            if (!isIncr && value instanceof Long && ((Long) value) < 0) {
                this.value = -((Long) value);
                this.isIncr = true;
            } else {
                this.value = value;
                this.isIncr = isIncr;
            }
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            appendName(name, codecRegistry, sb).append('=');
            appendName(name, codecRegistry, sb).append(isIncr ? "+" : "-");
            appendValue(value, codecRegistry, sb, variables);
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }

        @Override
        boolean isIdempotent() {
            return false;
        }
    }

    static class ListPrependAssignment extends Assignment {

        private final Object value;

        ListPrependAssignment(String name, Object value) {
            super(name);
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            appendName(name, codecRegistry, sb).append('=');
            appendValue(value, codecRegistry, sb, variables);
            sb.append('+');
            appendName(name, codecRegistry, sb);
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }

        @Override
        boolean isIdempotent() {
            return false;
        }
    }

    static class ListSetIdxAssignment extends Assignment {

        private final int idx;
        private final Object value;

        ListSetIdxAssignment(String name, int idx, Object value) {
            super(name);
            this.idx = idx;
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            appendName(name, codecRegistry, sb).append('[').append(idx).append("]=");
            appendValue(value, codecRegistry, sb, variables);
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }

        @Override
        boolean isIdempotent() {
            return true;
        }
    }

    static class CollectionAssignment extends Assignment {

        private final Object collection;
        private final boolean isAdd;
        private final boolean isIdempotent;

        CollectionAssignment(String name, Object collection, boolean isAdd, boolean isIdempotent) {
            super(name);
            this.collection = collection;
            this.isAdd = isAdd;
            this.isIdempotent = isIdempotent;
        }

        CollectionAssignment(String name, Object collection, boolean isAdd) {
            this(name, collection, isAdd, true);
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            appendName(name, codecRegistry, sb).append('=');
            appendName(name, codecRegistry, sb).append(isAdd ? "+" : "-");
            appendValue(collection, codecRegistry, sb, variables);
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(collection);
        }

        @Override
        public boolean isIdempotent() {
            return isIdempotent;
        }
    }

    static class MapPutAssignment extends Assignment {

        private final Object key;
        private final Object value;

        MapPutAssignment(String name, Object key, Object value) {
            super(name);
            this.key = key;
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            appendName(name, codecRegistry, sb).append('[');
            appendValue(key, codecRegistry, sb, variables);
            sb.append("]=");
            appendValue(value, codecRegistry, sb, variables);
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(key) || Utils.containsBindMarker(value);
        }

        @Override
        boolean isIdempotent() {
            return true;
        }
    }
}
