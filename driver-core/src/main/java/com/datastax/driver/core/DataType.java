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
    public Kind getKind();

    /**
     * Returns this type as a {@link Native} type.
     *
     * @return this type as a {@link Native} type.
     *
     * @throws IllegalStateException if this type is not a {@link Native} type.
     * You should use {@link #getKind} to check if this type is a native one
     * before calling this method.
     */
    public Native asNative();

    /**
     * Returns this type as a {@link Collection} type.
     *
     * @return this type as a {@link Collection} type.
     *
     * @throws IllegalStateException if this type is not a {@link Collection}
     * type. You should use {@link #getKind} to check if this type is a collection
     * one before calling this method.
     */
    public Collection asCollection();

    /**
     * Returns this type as a {@link Custom} type.
     *
     * @return this type as a {@link Custom} type.
     *
     * @throws IllegalStateException if this type is not a {@link Custom} type.
     * You should use {@link #getKind} to check if this type is a custom one
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

        public Kind getKind() { return Kind.NATIVE; }

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

        public Kind getKind() { return Kind.COLLECTION; }

        /**
         * The type of collection.
         *
         * @return the type of collection.
         */
        public Type collectionType() { return type; }

        public Native asNative()         { throw new IllegalStateException("Not a native type, but a collection one"); }
        public Collection asCollection() { return this; }
        public Custom asCustom()         { throw new IllegalStateException("Not a custom type, but a collection one"); }

        /**
         * The type of lists.
         */
        public static class List extends Collection {
            private final DataType elementsType;

            /**
             * Creates a list type with the provided element type.
             *
             * @param elementsType the type of the elements of the list.
             */
            public List(DataType elementsType) {
                super(Type.LIST);
                this.elementsType = elementsType;
            }

            /**
             * The data type of the elements for this list type.
             *
             * @return the data type of the elements for this list type.
             */
            public DataType getElementsType() {
                return elementsType;
            }

            @Override
            public String toString() {
                return "list<" + elementsType + ">";
            }
        }

        /**
         * The type of sets.
         */
        public static class Set extends Collection {
            private final DataType elementsType;

            /**
             * Creates a set type with the provided element type.
             *
             * @param elementsType the type of the elements of the set.
             */
            public Set(DataType elementsType) {
                super(Type.SET);
                this.elementsType = elementsType;
            }

            /**
             * The data type of the elements for this set type.
             *
             * @return the data type of the elements for this set type.
             */
            public DataType getElementsType() {
                return elementsType;
            }

            @Override
            public String toString() {
                return "list<" + elementsType + ">";
            }
        }

        /**
         * The type of maps.
         */
        public static class Map extends Collection {
            private final DataType keysType;
            private final DataType valuesType;

            /**
             * Creates a map type with the provided key and value type.
             *
             * @param keysType the type of the keys of the map.
             * @param valuesType the type of the keys of the map.
             */
            public Map(DataType keysType, DataType valuesType) {
                super(Type.MAP);
                this.keysType = keysType;
                this.valuesType = valuesType;
            }

            /**
             * The data type of the keys for this map type.
             *
             * @return the data type of the keys for this map type.
             */
            public DataType getKeysType() {
                return keysType;
            }

            /**
             * The data type of the values for this map type.
             *
             * @return the data type of the values for this map type.
             */
            public DataType getValuesType() {
                return valuesType;
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

        public Kind getKind() { return Kind.CUSTOM; }

        public Native asNative()         { throw new IllegalStateException("Not a native type, but a custom one"); }
        public Collection asCollection() { throw new IllegalStateException("Not a collection type, but a custom one"); }
        public Custom asCustom()         { return this; }
    }
}
