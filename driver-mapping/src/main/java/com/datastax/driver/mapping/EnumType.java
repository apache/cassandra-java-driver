package com.datastax.driver.mapping;

/**
 * The types of way to persist a JAVA Enum.
 */
public enum EnumType {
    /** Persists enumeration values using their ordinal in the Enum declaration. */
    ORDINAL,
    /** Persists enumeration values using the string they have been declared with. */
    STRING
}
