/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class ColumnDefinitionsTest {

    @Test(groups = "unit")
    public void caseTest() {

        ColumnDefinitions defs;

        defs = new ColumnDefinitions(new ColumnDefinitions.Definition[]{
                new ColumnDefinitions.Definition("ks", "cf", "aColumn", DataType.text()),
                new ColumnDefinitions.Definition("ks", "cf", "fOO", DataType.text()),
                new ColumnDefinitions.Definition("ks", "cf", "anotherColumn", DataType.text())
        }, CodecRegistry.DEFAULT_INSTANCE);

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
        }, CodecRegistry.DEFAULT_INSTANCE);

        assertTrue(defs.getType("foo").equals(DataType.text()));
        assertTrue(defs.getType("Foo").equals(DataType.text()));
        assertTrue(defs.getType("FOO").equals(DataType.text()));
        assertTrue(defs.getType("\"FOO\"").equals(DataType.cint()));

        assertTrue(defs.contains("with \" quote"));

        assertTrue(defs.getType("in quote").equals(DataType.cint()));
        assertTrue(defs.getType("\"in quote\"").equals(DataType.cint()));
        assertTrue(defs.getType("\"\"in quote\"\"").equals(DataType.text()));
    }

    @Test(groups = "unit")
    public void multiDefinitionTest() {

        ColumnDefinitions defs = new ColumnDefinitions(new ColumnDefinitions.Definition[]{
                new ColumnDefinitions.Definition("ks", "cf1", "column", DataType.text()),
                new ColumnDefinitions.Definition("ks", "cf2", "column", DataType.cint()),
                new ColumnDefinitions.Definition("ks", "cf3", "column", DataType.cfloat())
        }, CodecRegistry.DEFAULT_INSTANCE);

        assertTrue(defs.getType("column").equals(DataType.text()));
    }
}
