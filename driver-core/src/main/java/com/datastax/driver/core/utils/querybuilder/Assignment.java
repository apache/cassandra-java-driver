package com.datastax.driver.core.utils.querybuilder;

import java.util.*;

import static com.datastax.driver.core.utils.querybuilder.Utils.*;

public abstract class Assignment extends Utils.Appendeable {

    protected final String name;

    private Assignment(String name) {
        this.name = name;
    }

    String name() {
        return name;
    }

    public static <T> Assignment set(String name, T value) {
        return new SetAssignment(name, value);
    }

    public static Assignment incr(String name) {
        return incr(name, 1L);
    }

    public static Assignment incr(String name, long value) {
        return new CounterAssignment(name, value, true);
    }

    public static Assignment decr(String name) {
        return decr(name, 1L);
    }

    public static Assignment decr(String name, long value) {
        return new CounterAssignment(name, value, false);
    }

    public static <T> Assignment prepend(String name, T value) {
        return new ListPrependAssignment(name, Collections.singletonList(value));
    }

    public static <T> Assignment prependAll(String name, List<T> list) {
        return new ListPrependAssignment(name, list);
    }

    public static <T> Assignment append(String name, T value) {
        return new CollectionAssignment(name, Collections.singletonList(value), true);
    }

    public static <T> Assignment appendAll(String name, List<T> list) {
        return new CollectionAssignment(name, list, true);
    }

    public static <T> Assignment discard(String name, T value) {
        return new CollectionAssignment(name, Collections.singletonList(value), false);
    }

    public static <T> Assignment discardAll(String name, List<T> list) {
        return new CollectionAssignment(name, list, false);
    }

    public static <T> Assignment setIdx(String name, int idx, T value) {
        return new ListSetIdxAssignment(name, idx, value);
    }

    public static <T> Assignment add(String name, T value) {
        return new CollectionAssignment(name, Collections.singleton(value), true);
    }

    public static <T> Assignment addAll(String name, Set<T> set) {
        return new CollectionAssignment(name, set, true);
    }

    public static <T> Assignment remove(String name, T value) {
        return new CollectionAssignment(name, Collections.singleton(value), false);
    }

    public static <T> Assignment removeAll(String name, Set<T> set) {
        return new CollectionAssignment(name, set, false);
    }

    public static <K,V> Assignment put(String name, K key, V value) {
        return new MapPutAssignment(name, key, value);
    }

    public static <K,V> Assignment putAll(String name, Map<K,V> value) {
        return new CollectionAssignment(name, value, true);
    }

    private static class SetAssignment extends Assignment {

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

        Object firstValue() {
            return value;
        }
    }

    private static abstract class NoRoutingAssignment extends Assignment {

        NoRoutingAssignment(String name) {
            super(name);
        }

        @Override
        String name() {
            // This can't be a routing key
            return null;
        }

        String firstValue() {
            return null;
        }
    }

    private static class CounterAssignment extends NoRoutingAssignment {

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

    private static class ListPrependAssignment extends NoRoutingAssignment {

        private final List value;

        ListPrependAssignment(String name, List value) {
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

    private static class ListSetIdxAssignment extends NoRoutingAssignment {

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

    private static class CollectionAssignment extends NoRoutingAssignment {

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

    private static class MapPutAssignment extends NoRoutingAssignment {

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
