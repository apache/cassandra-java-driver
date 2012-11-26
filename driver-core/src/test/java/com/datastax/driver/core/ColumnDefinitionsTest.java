package com.datastax.driver.core;

import org.junit.Test;
import static junit.framework.Assert.*;

public class ColumnDefinitionsTest {

    @Test
    public void caseTest() {

        ColumnDefinitions defs;

        defs = new ColumnDefinitions(new ColumnDefinitions.Definition[]{
            new ColumnDefinitions.Definition("ks", "cf", "aColumn", DataType.text()),
            new ColumnDefinitions.Definition("ks", "cf", "fOO", DataType.text()),
            new ColumnDefinitions.Definition("ks", "cf", "anotherColumn", DataType.text())
        });

        assertTrue(defs.contains("foo"));
        assertTrue(defs.contains("fOO"));
        assertTrue(defs.contains("FOO"));

        defs = new ColumnDefinitions(new ColumnDefinitions.Definition[]{
            new ColumnDefinitions.Definition("ks", "cf", "aColumn", DataType.text()),
            new ColumnDefinitions.Definition("ks", "cf", "foo", DataType.text()),
            new ColumnDefinitions.Definition("ks", "cf", "anotherColumn", DataType.text()),
            new ColumnDefinitions.Definition("ks", "cf", "FOO", DataType.cint()),
            new ColumnDefinitions.Definition("ks", "cf", "with \" quote", DataType.text()),
            new ColumnDefinitions.Definition("ks", "cf", "\"in quote\"", DataType.text()),
            new ColumnDefinitions.Definition("ks", "cf", "in quote", DataType.cint()),
        });

        assertTrue(defs.getType("foo").equals(DataType.text()));
        assertTrue(defs.getType("Foo").equals(DataType.text()));
        assertTrue(defs.getType("FOO").equals(DataType.text()));
        assertTrue(defs.getType("\"FOO\"").equals(DataType.cint()));

        assertTrue(defs.contains("with \" quote"));

        assertTrue(defs.getType("in quote").equals(DataType.cint()));
        assertTrue(defs.getType("\"in quote\"").equals(DataType.cint()));
        assertTrue(defs.getType("\"\"in quote\"\"").equals(DataType.text()));
    }
}
