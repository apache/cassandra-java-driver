package com.datastax.driver.core;

/**
 * Supported data types for columns.
 */
public interface DataType {

    /**
     * The three kind of type supported by Cassandra.
     *
     * The {@code NATIVE} types supported by Cassandra are described in the
     * <a href="http://cassandra.apache.org/doc/cql3/CQL.html#types">CQL documentation</a>,
     * and more information on such type can be obtained using the {#asNative}
     * method.
     *
     * The {@code COLLECTION} types the maps, lists and sets. More information
     * on such type can be obtained using the {#asCollection} method.
     *
     * The {@code CUSTOM} types are user defined types. More information on
     * such type can be obtained using the {#asCustom} method.
     */
    public enum Kind { NATIVE, COLLECTION, CUSTOM }

    /**
     * Returns this type {@link Kind}.
     *
     * @return this type {@link Kind}.
     */
    public Kind kind();

    /**
     * Returns this type as a {@link Native} type.
     *
     * @return this type as a {@link Native} type.
     *
     * @throws IllegalStateException if this type is not a {@link Native} type.
     * You should use {@link #kind} to check if this type is a native one
     * before calling this method.
     */
    public Native asNative();

    /**
     * Returns this type as a {@link Collection} type.
     *
     * @return this type as a {@link Collection} type.
     *
     * @throws IllegalStateException if this type is not a {@link Collection}
     * type. You should use {@link #kind} to check if this type is a collection
     * one before calling this method.
     */
    public Collection asCollection();

    /**
     * Returns this type as a {@link Custom} type.
     *
     * @return this type as a {@link Custom} type.
     *
     * @throws IllegalStateException if this type is not a {@link Custom} type.
     * You should use {@link #kind} to check if this type is a custom one
     * before calling this method.
     */
    public Custom asCustom();

    /**
     * Native types supported by cassandra.
     */
    public enum Native implements DataType {

        ASCII,
        BIGINT,
        BLOB,
        BOOLEAN,
        COUNTER,
        DECIMAL,
        DOUBLE,
        FLOAT,
        INET,
        INT,
        TEXT,
        TIMESTAMP,
        UUID,
        VARCHAR,
        VARINT,
        TIMEUUID;

        public Kind kind() { return Kind.NATIVE; }

        public Native asNative()         { return this; }
        public Collection asCollection() { throw new IllegalStateException("Not a collection type, but a native one"); }
        public Custom asCustom()         { throw new IllegalStateException("Not a custom type, but a native one"); }

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    /**
     * A collection type (lists, sets and maps).
     */
    public static abstract class Collection implements DataType {

        // TODO: Type is a very ugly/confusing name
        public enum Type { LIST, SET, MAP };

        private final Type type;

        protected Collection(Type type) {
            this.type = type;
        }

        public Kind kind() { return Kind.COLLECTION; }

        public Type collectionType() { return type; }

        public Native asNative()         { throw new IllegalStateException("Not a native type, but a collection one"); }
        public Collection asCollection() { return this; }
        public Custom asCustom()         { throw new IllegalStateException("Not a custom type, but a collection one"); }

        public static class List extends Collection {
            private final DataType elementsType;

            public List(DataType elementsType) {
                super(Type.LIST);
                this.elementsType = elementsType;
            }

            public DataType getElementsType() {
                return elementsType;
            }

            @Override
            public String toString() {
                return "list<" + elementsType + ">";
            }
        }

        public static class Set extends Collection {
            private final DataType elementsType;

            public Set(DataType elementsType) {
                super(Type.SET);
                this.elementsType = elementsType;
            }

            public DataType getElementsType() {
                return elementsType;
            }

            @Override
            public String toString() {
                return "list<" + elementsType + ">";
            }
        }

        public static class Map extends Collection {
            private final DataType keysType;
            private final DataType valuesType;

            public Map(DataType keysType, DataType valuesType) {
                super(Type.MAP);
                this.keysType = keysType;
                this.valuesType = valuesType;
            }

            public DataType getKeysType() {
                return keysType;
            }

            public DataType getValuesType() {
                return keysType;
            }

            @Override
            public String toString() {
                return "map<" + keysType + ", " + valuesType + ">";
            }
        }
    }

    /**
     * A used defined custom type.
     */
    public static class Custom implements DataType {
        // TODO

        public Kind kind() { return Kind.CUSTOM; }

        public Native asNative()         { throw new IllegalStateException("Not a native type, but a custom one"); }
        public Collection asCollection() { throw new IllegalStateException("Not a collection type, but a custom one"); }
        public Custom asCustom()         { return this; }
    }
}
