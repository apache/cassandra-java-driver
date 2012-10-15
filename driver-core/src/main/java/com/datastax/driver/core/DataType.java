package com.datastax.driver.core;

/**
 * Supported data types for columns.
 */
public interface DataType {

    /**
     * A Cassandra type.
     *
     * There is two family of type: the native ones and the collection ones.
     * One can decide if the type is a native type of a collection one using
     * the {@link #isCollection method}.
     *
     * The {@code NATIVE} types are described in the
     * <a href="http://cassandra.apache.org/doc/cql3/CQL.html#types">CQL documentation</a>.
     *
     * The {@code COLLECTION} types are the maps, lists and sets.
     */
    public enum Kind { NATIVE, COLLECTION, CUSTOM }

    /**
     * Returns whether this type is a collection type.
     *
     * @return {@code true} if the type is a collection one, {@code false} if
     * it is a native type.
     */
    public boolean isCollection();

    /**
     * Returns this type as a {@link Native} type.
     *
     * @return this type as a {@link Native} type.
     *
     * @throws IllegalStateException if this type is not a {@link Native} type.
     * You should use {@link #isCollection} to check if this type is a native one
     * before calling this method.
     */
    public Native asNative();

    /**
     * Returns this type as a {@link Collection} type.
     *
     * @return this type as a {@link Collection} type.
     *
     * @throws IllegalStateException if this type is not a {@link Collection}
     * type. You should use {@link #isCollection} to check if this type is a
     * collection one before calling this method.
     */
    public Collection asCollection();

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

        public boolean isCollection()    { return false; }
        public Native asNative()         { return this; }
        public Collection asCollection() { throw new IllegalStateException("Not a collection type, but a native one"); }

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    /**
     * A collection type (lists, sets and maps).
     */
    public static abstract class Collection implements DataType {

        /**
         * The kind of collection a collection type represents.
         */
        public enum Kind { LIST, SET, MAP };

        private final Kind kind;

        protected Collection(Kind kind) {
            this.kind = kind;
        }

        public boolean isCollection() { return true; }

        /**
         * The kind of collection this type represents.
         *
         * @return the kind of collection (list, set or map) this type
         * represents.
         */
        public Kind getKind() { return kind; }

        public Native asNative()         { throw new IllegalStateException("Not a native type, but a collection one"); }
        public Collection asCollection() { return this; }

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
                super(Kind.LIST);
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
                super(Kind.SET);
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
                super(Kind.MAP);
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
}
