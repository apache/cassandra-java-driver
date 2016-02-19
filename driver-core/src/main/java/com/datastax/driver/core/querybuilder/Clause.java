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

import com.datastax.driver.core.CodecRegistry;

import java.util.List;

public abstract class Clause extends Utils.Appendeable {

    abstract String name();

    abstract Object firstValue();

    private static abstract class AbstractClause extends Clause {
        final String name;

        private AbstractClause(String name) {
            this.name = name;
        }

        @Override
        String name() {
            return name;
        }
    }

    static class SimpleClause extends AbstractClause {

        private final String op;
        private final Object value;

        SimpleClause(String name, String op, Object value) {
            super(name);
            this.op = op;
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            Utils.appendName(name, sb).append(op);
            Utils.appendValue(value, codecRegistry, sb, variables);
        }

        @Override
        Object firstValue() {
            return value;
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }
    }

    static class InClause extends AbstractClause {

        private final List<?> values;

        InClause(String name, List<?> values) {
            super(name);
            this.values = values;

            if (values == null)
                throw new IllegalArgumentException("Missing values for IN clause");
            if (values.size() > 65535)
                throw new IllegalArgumentException("Too many values for IN clause, the maximum allowed is 65535");
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {

            // We special case the case of just one bind marker because there is little
            // reasons to do:
            //    ... IN (?) ...
            // since in that case it's more elegant to use an equal. On the other side,
            // it is a lot more useful to do:
            //    ... IN ? ...
            // which binds the variable to the full list the IN is on.
            if (values.size() == 1 && values.get(0) instanceof BindMarker) {
                Utils.appendName(name, sb).append(" IN ").append(values.get(0));
                return;
            }

            Utils.appendName(name, sb).append(" IN (");
            Utils.joinAndAppendValues(sb, codecRegistry, ",", values, variables).append(')');
        }

        @Override
        Object firstValue() {
            return values.isEmpty() ? null : values.get(0);
        }

        @Override
        boolean containsBindMarker() {
            for (Object value : values)
                if (Utils.containsBindMarker(value))
                    return true;
            return false;
        }
    }

    static class ContainsClause extends AbstractClause {

        private final Object value;

        ContainsClause(String name, Object value) {
            super(name);
            this.value = value;

            if (value == null)
                throw new IllegalArgumentException("Missing value for CONTAINS clause");
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            Utils.appendName(name, sb).append(" CONTAINS ");
            Utils.appendValue(value, codecRegistry, sb, variables);
        }

        @Override
        Object firstValue() {
            return value;
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }
    }


    static class ContainsKeyClause extends AbstractClause {

        private final Object value;

        ContainsKeyClause(String name, Object value) {
            super(name);
            this.value = value;

            if (value == null)
                throw new IllegalArgumentException("Missing value for CONTAINS KEY clause");
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            Utils.appendName(name, sb).append(" CONTAINS KEY ");
            Utils.appendValue(value, codecRegistry, sb, variables);
        }

        @Override
        Object firstValue() {
            return value;
        }

        @Override
        boolean containsBindMarker() {
            return Utils.containsBindMarker(value);
        }
    }


    static class CompoundClause extends Clause {
        private String op;
        private final List<String> names;
        private final List<?> values;

        CompoundClause(List<String> names, String op, List<?> values) {
            assert names.size() == values.size();
            this.op = op;
            this.names = names;
            this.values = values;
        }

        @Override
        String name() {
            // This is only used for routing key purpose, and so far CompoundClause
            // are not allowed for the partitionKey anyway
            return null;
        }

        @Override
        Object firstValue() {
            // This is only used for routing key purpose, and so far CompoundClause
            // are not allowed for the partitionKey anyway
            return null;
        }

        @Override
        boolean containsBindMarker() {
            for (Object value : values)
                if (Utils.containsBindMarker(value))
                    return true;
            return false;
        }

        @Override
        void appendTo(StringBuilder sb, List<Object> variables, CodecRegistry codecRegistry) {
            sb.append("(");
            for (int i = 0; i < names.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Utils.appendName(names.get(i), sb);
            }
            sb.append(")").append(op).append("(");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Utils.appendValue(values.get(i), codecRegistry, sb, variables);
            }
            sb.append(")");
        }
    }
}
