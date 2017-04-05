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

import java.util.List;

/**
 * Represent a naming convention (e.g. snake_case, camelCase, etc...) to be used when
 * auto-translating java property names to cassandra column names and vice versa.
 * <p>
 * This interface may be implemented to define custom naming convention.
 */
public interface NamingConvention {

    /**
     * Receive a property name value and returns an ordered list of Word objects.
     * Each word contains a String value and a boolean indicating whether or not
     * the value is an abbreviation (In most cases could not be determined).
     * Quick examples:
     * <ul>
     * <li>Let's consider lowerCamelCase convention and input = "myXMLParser",
     * then the output should be:
     * [
     * word{value = "my", isAbbreviation = false},
     * word{value = "xml", isAbbreviation = true},
     * word{value = "parser", isAbbreviation = false}
     * ]</li>
     * <li>Let's consider lower_snake_case convention and input = "myXMLParser",
     * then the output may be (since there's no trivial way to determine xml
     * to an abbreviation):
     * [
     * word{value = "my", isAbbreviation = false},
     * word{value = "xml", isAbbreviation = false},
     * word{value = "parser", isAbbreviation = false}
     * ]</li>
     * </ul>
     *
     * @param input value to split
     * @return an ordered list of split Word objects
     */
    List<Word> split(String input);

    /**
     * Receive an ordered list of Word objects and returns a result property name.
     * Quick examples:
     * <ul>
     * <li>Let's consider lowerCamelCase convention with upperCaseAbbreviations set
     * to false, and input = [
     * word{value = "my", isAbbreviation = false},
     * word{value = "xml", isAbbreviation = true},
     * word{value = "parser", isAbbreviation = false}
     * ]
     * then the output should be "myXmlParser".</li>
     * <li>Let's consider upperCamelCase convention with upperCaseAbbreviations set
     * to true, and input = [
     * word{value = "my", isAbbreviation = false},
     * word{value = "xml", isAbbreviation = true},
     * word{value = "parser", isAbbreviation = false}
     * ]
     * then the output should be "MyXMLParser".</li>
     * </ul>
     *
     * @param input list to translate
     * @return the result property name
     */
    String join(List<Word> input);

}
