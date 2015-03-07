package com.datastax.driver.mapping;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class DeclaredFrozenTypeTest {
    @Test(groups = "unit")
    public void parseSimpleTypeTest() {
        DeclaredFrozenType type = DeclaredFrozenType.parse(" foo");
        assertEquals(type.name, "foo");
        assertFalse(type.frozen);
        assertNull(type.subTypes);
    }

    @Test(groups = "unit")
    public void parseQuotedTypeTest() {
        DeclaredFrozenType type = DeclaredFrozenType.parse("\"Foo bar\"");
        assertEquals(type.name, "Foo bar");
        assertFalse(type.frozen);
        assertNull(type.subTypes);
    }

    @Test(groups = "unit")
    public void parseNestedTypeTest() {
        // list
        DeclaredFrozenType type = DeclaredFrozenType.parse("list<foo1>");
        assertEquals(type.name, "list");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 1);

        DeclaredFrozenType subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "foo1");
        assertFalse(subType0.frozen);
        assertNull(subType0.subTypes);

        // map
        type = DeclaredFrozenType.parse("map < foo , bar >");
        assertEquals(type.name, "map");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 2);

        subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "foo");
        assertFalse(subType0.frozen);
        assertNull(subType0.subTypes);

        DeclaredFrozenType subType1 = type.subTypes.get(1);
        assertEquals(subType1.name, "bar");
        assertFalse(subType1.frozen);
        assertNull(subType1.subTypes);

    }

    @Test(groups = "unit")
    public void parseSimpleFrozenTypeTest() {
        DeclaredFrozenType type = DeclaredFrozenType.parse("frozen<foo_1>");
        assertEquals(type.name, "foo_1");
        assertTrue(type.frozen);
        assertNull(type.subTypes);
    }

    @Test(groups = "unit")
    public void parseNestedFrozenTypeTest() {
        DeclaredFrozenType type = DeclaredFrozenType.parse("list<frozen<foo>>");
        assertEquals(type.name, "list");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 1);

        DeclaredFrozenType subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "foo");
        assertTrue(subType0.frozen);
        assertNull(subType0.subTypes);
    }

    @Test(groups = "unit")
    public void parseDeeplyNestedTypeTest() {
        // NB: nested collections are not allowed in C* 2.1, but might be in the future, so we want to handle that
        DeclaredFrozenType type = DeclaredFrozenType.parse("map<text, map<text, frozen<foo>>>");
        assertEquals(type.name, "map");
        assertFalse(type.frozen);
        assertEquals(type.subTypes.size(), 2);

        DeclaredFrozenType subType0 = type.subTypes.get(0);
        assertEquals(subType0.name, "text");
        assertFalse(subType0.frozen);
        assertNull(subType0.subTypes);

        DeclaredFrozenType subType1 = type.subTypes.get(1);
        assertEquals(subType1.name, "map");
        assertFalse(subType1.frozen);
        assertEquals(subType1.subTypes.size(), 2);

        DeclaredFrozenType subType10 = subType1.subTypes.get(0);
        assertEquals(subType10.name, "text");
        assertFalse(subType10.frozen);
        assertNull(subType10.subTypes);

        DeclaredFrozenType subType11 = subType1.subTypes.get(1);
        assertEquals(subType11.name, "foo");
        assertTrue(subType11.frozen);
        assertNull(subType11.subTypes);
    }
}
