package com.datastax.driver.mapping;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class CQLTypeTest {
    @Test(groups = "unit")
    public void parseSimpleTypeTest() {
        CQLType type = CQLType.parse(" foo");
        assertEquals(type.name, "foo");
        assertFalse(type.frozen);
        assertNull(type.subTypes);
    }

    @Test(groups = "unit")
    public void parseQuotedTypeTest() {
        CQLType type = CQLType.parse("\"Foo bar\"");
        assertEquals(type.name, "Foo bar");
        assertFalse(type.frozen);
        assertNull(type.subTypes);
    }

    @Test(groups = "unit")
    public void parseNestedTypeTest() {
        // list
        CQLType type = CQLType.parse("list<foo1>");
        assertEquals(type.name, "list");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 1);

        CQLType subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "foo1");
        assertFalse(subType0.frozen);
        assertNull(subType0.subTypes);

        // map
        type = CQLType.parse("map < foo , bar >");
        assertEquals(type.name, "map");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 2);

        subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "foo");
        assertFalse(subType0.frozen);
        assertNull(subType0.subTypes);

        CQLType subType1 = type.subTypes.get(1);
        assertEquals(subType1.name, "bar");
        assertFalse(subType1.frozen);
        assertNull(subType1.subTypes);

    }

    @Test(groups = "unit")
    public void parseSimpleFrozenTypeTest() {
        CQLType type = CQLType.parse("frozen<foo_1>");
        assertEquals(type.name, "foo_1");
        assertTrue(type.frozen);
        assertNull(type.subTypes);
    }

    @Test(groups = "unit")
    public void parseNestedFrozenTypeTest() {
        CQLType type = CQLType.parse("list<frozen<foo>>");
        assertEquals(type.name, "list");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 1);

        CQLType subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "foo");
        assertTrue(subType0.frozen);
        assertNull(subType0.subTypes);
    }

    @Test(groups = "unit")
    public void parseDeeplyNestedTypeTest() {
        // NB: nested collections are not allowed in C* 2.1, but might be in the future, so we want to handle that
        CQLType type = CQLType.parse("map<text, map<text, frozen<foo>>>");
        assertEquals(type.name, "map");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 2);

        CQLType subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "text");
        assertFalse(subType0.frozen);
        assertNull(subType0.subTypes);

        CQLType subType1 = type.subTypes.get(1);
        assertEquals(subType1.name, "map");
        assertFalse(subType1.frozen);
        assertEquals(subType1.subTypes.size(), 2);

        CQLType subType10 = subType1.subTypes.get(0);
        assertEquals(subType10.name, "text");
        assertFalse(subType10.frozen);
        assertNull(subType10.subTypes);

        CQLType subType11 = subType1.subTypes.get(1);
        assertEquals(subType11.name, "foo");
        assertTrue(subType11.frozen);
        assertNull(subType11.subTypes);
    }
}
