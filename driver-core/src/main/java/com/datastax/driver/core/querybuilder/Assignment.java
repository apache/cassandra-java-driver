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

import java.util.*;

import static com.datastax.driver.core.querybuilder.Utils.*;

public abstract class Assignment extends Utils.Appendeable {

    protected final String name;

    private Assignment(String name) {
        this.name = name;
    };

    static class SetAssignment extends Assignment {

        private final Object value;

        SetAssignment(String name, Object value) {
            super(name);
            this.value = value;
        }

        void appendTo(StringBuilder sb) {
            appendName(name, sb);
            sb.append("=");
            appendValue(value, sb);
        }
    }

    static class CounterAssignment extends Assignment {

        private final long value;
        private final boolean isIncr;

        CounterAssignment(String name, long value, boolean isIncr) {
            super(name);
            if (!isIncr && value < 0) {
                this.value = -value;
                this.isIncr = true;
            } else {
                this.value = value;
                this.isIncr = isIncr;
            }
        }

        void appendTo(StringBuilder sb) {
            appendName(name, sb).append("=");
            appendName(name, sb).append(isIncr ? "+" : "-").append(value);
        }

    }

    static class ListPrependAssignment extends Assignment {

        private final List<?> value;

        ListPrependAssignment(String name, List<?> value) {
            super(name);
            this.value = value;
        }

        void appendTo(StringBuilder sb) {
            appendName(name, sb).append("=");
            appendList(value, sb);
            sb.append("+");
            appendName(name, sb);
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

        void appendTo(StringBuilder sb) {
            appendName(name, sb).append("[").append(idx).append("]=");
            appendValue(value, sb);
        }
    }

    static class CollectionAssignment extends Assignment {

        private final Object collection;
        private final boolean isAdd;

        CollectionAssignment(String name, Object collection, boolean isAdd) {
            super(name);
            this.collection = collection;
            this.isAdd = isAdd;
        }

        void appendTo(StringBuilder sb) {
            appendName(name, sb).append("=");
            appendName(name, sb).append(isAdd ? "+" : "-");
            appendCollection(collection, sb);
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

        void appendTo(StringBuilder sb) {
            appendName(name, sb).append("[");
            appendValue(key, sb);
            sb.append("]=");
            appendValue(value, sb);
        }
    }
}
