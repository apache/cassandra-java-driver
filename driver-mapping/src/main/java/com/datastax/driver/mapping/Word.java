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
package com.datastax.driver.mapping;

/**
 * Represents a single-individual word in a property name. (e.g. "my", "xml" and "parser" in a
 * property named "myXmlParser").
 * Each word contains a String value and a boolean indicating whether or not
 * the value is an abbreviation.
 * In most cases there will be no trivial way to identify abbreviations
 * (i.e. my_xml_parser in snake case), but for some naming conventions this may be helpful.
 */
public class Word {

    private final String value;

    private final boolean isAbbreviation;

    public Word(String value, boolean isAbbreviation) {
        this.value = value;
        this.isAbbreviation = isAbbreviation;
    }

    public Word(String value) {
        this(value, false);
    }

    public String getValue() {
        return value;
    }

    public boolean isAbbreviation() {
        return isAbbreviation;
    }

}
