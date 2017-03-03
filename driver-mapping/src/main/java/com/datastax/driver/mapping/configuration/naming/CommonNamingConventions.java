/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping.configuration.naming;

import java.util.*;

/**
 * Implementations of industry common naming conventions
 */
public class CommonNamingConventions {

    /**
     * @link "https://en.wikipedia.org/wiki/Camel_case"
     *
     * Represents camel case naming convention with a lower cased first letter.
     * E.g. "myXmlParser" and "myXMLParser", Note that both examples are valid
     * lower camel case forms. The first one takes abbreviations as any other
     * words where the first letter is upper case, and the rest are lower case
     * (hence - "Xml"), while the latter upper cases all letters of an abbreviation
     * (hence - "XML").
     *
     * Additionally, many different Java naming conventions introduce prefixes
     * for field naming, some examples:
     * <ul>
     *     <li>Android - @link "http://source.android.com/source/code-style.html#follow-field-naming-conventions"</li>
     *     <li>Hungarian Notation - @link "https://en.wikipedia.org/wiki/Hungarian_notation"</li>
     *     <li>Underscore - @link "http://stackoverflow.com/questions/1899683/is-there-a-standard-in-java-for-underscore-in-front-of-variable-or-class-nam"</li>
     * </ul>
     * Those prefixes can be supported so that for example if java convention is
     * set to LowerCamelCase with `ignorablePrefixes` set to "_" and cassandra
     * convention is set to LowerSnakeCase, then a field named "_myXmlParser" will be
     * translated to "my_xml_parser".
     */
    public static class LowerCamelCase extends CamelCase {

        private final boolean upperCaseAbbreviations;

        /**
         * @param upperCaseAbbreviations  {@code true} to uppercase all abbreviations,
         *                                {@code false} to treat abbreviations as any other word
         * @param ignorablePrefixes       string prefixes to trim if constant field name prefixes are used
         */
        public LowerCamelCase(boolean upperCaseAbbreviations, String... ignorablePrefixes) {
            super(ignorablePrefixes);
            this.upperCaseAbbreviations = upperCaseAbbreviations;
        }

        /**
         * @param ignorablePrefixes       string prefixes to trim if constant field name prefixes are used
         */
        public LowerCamelCase(String... ignorablePrefixes) {
            this(false, ignorablePrefixes);
        }

        /**
         * @param upperCaseAbbreviations  {@code true} to uppercase all abbreviations,
         *                                {@code false} to treat abbreviations as any other word
         */
        public LowerCamelCase(boolean upperCaseAbbreviations) {
            this(upperCaseAbbreviations, new String[0]);
        }

        @Override
        public String join(List<Word> input) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < input.size(); i++) {
                Word word = input.get(i);
                String value;
                if (i == 0) {
                    value = word.getValue().toLowerCase();
                }
                else if (upperCaseAbbreviations && word.isAbbreviation()) {
                    value = word.getValue().toUpperCase();
                }
                else {
                    value = word.getValue().substring(0, 1).toUpperCase() + word.getValue().substring(1).toLowerCase();
                }
                builder.append(value);
            }
            return builder.toString();
        }

        @Override
        public boolean equals(NamingConvention other) {
            return other instanceof LowerCamelCase &&
                    ((LowerCamelCase)other).upperCaseAbbreviations == upperCaseAbbreviations &&
                    super.equals(other);
        }

    }

    /**
     * @link "https://en.wikipedia.org/wiki/Camel_case"
     *
     * Represents camel case naming convention with an upper cased first letter.
     * E.g. "MyXmlParser" and "MyXMLParser", Note that both examples are valid
     * upper camel case forms. The first one takes abbreviations as any other
     * words where the first letter is upper case, and the rest are lower case
     * (hence - "Xml"), while the latter upper cases all letters of an abbreviation
     * (hence - "XML").
     *
     * Additionally, many different Java naming conventions introduce prefixes
     * for field naming, some examples:
     * <ul>
     *     <li>Android - @link "http://source.android.com/source/code-style.html#follow-field-naming-conventions"</li>
     *     <li>Hungarian Notation - @link "https://en.wikipedia.org/wiki/Hungarian_notation"</li>
     *     <li>Underscore - @link "http://stackoverflow.com/questions/1899683/is-there-a-standard-in-java-for-underscore-in-front-of-variable-or-class-nam"</li>
     * </ul>
     * Those prefixes can be supported so that for example if java convention is
     * set to UpperCamelCase with `ignorablePrefixes` set to "_" and cassandra
     * convention is set to LowerSnakeCase, then a field named "_MyXmlParser" will be
     * translated to "my_xml_parser".
     */
    public static class UpperCamelCase extends CamelCase {

        private final boolean upperCaseAbbreviations;

        /**
         * @param upperCaseAbbreviations  {@code true} to uppercase all abbreviations,
         *                                {@code false} to treat abbreviations as any other word
         * @param ignorablePrefixes       string prefixes to trim if constant field name prefixes are used
         */
        public UpperCamelCase(boolean upperCaseAbbreviations, String... ignorablePrefixes) {
            super(ignorablePrefixes);
            this.upperCaseAbbreviations = upperCaseAbbreviations;
        }

        /**
         * @param ignorablePrefixes       string prefixes to trim if constant field name prefixes are used
         */
        public UpperCamelCase(String... ignorablePrefixes) {
            this(false, ignorablePrefixes);
        }

        /**
         * @param upperCaseAbbreviations  {@code true} to uppercase all abbreviations,
         *                                {@code false} to treat abbreviations as any other word
         */
        public UpperCamelCase(boolean upperCaseAbbreviations) {
            this(upperCaseAbbreviations, new String[0]);
        }

        @Override
        public String join(List<Word> input) {
            StringBuilder builder = new StringBuilder();
            for (Word word : input) {
                String value;
                if (upperCaseAbbreviations && word.isAbbreviation()) {
                    value = word.getValue().toUpperCase();
                }
                else {
                    value = word.getValue().substring(0, 1).toUpperCase() + word.getValue().substring(1).toLowerCase();
                }
                builder.append(value);
            }
            return builder.toString();
        }

        @Override
        public boolean equals(NamingConvention other) {
            return other instanceof UpperCamelCase &&
                    ((UpperCamelCase)other).upperCaseAbbreviations == upperCaseAbbreviations &&
                    super.equals(other);
        }

    }

    /**
     * @link "https://en.wikipedia.org/wiki/Snake_case"
     *
     * Represents snake case naming convention, meaning all letters are lower cased,
     * and words are separated by an underscore ("_"). E.g. "my_xml_parser".
     */
    public static class LowerSnakeCase extends CharDelimitedNamingConvention {

        public LowerSnakeCase() {
            super("_", false);
        }

    }

    /**
     * @link "https://en.wikipedia.org/wiki/Snake_case"
     *
     * Represents snake case naming convention, meaning all letters are upper cased,
     * and words are separated by an underscore ("_"). E.g. "MY_XML_PARSER".
     */
    public static class UpperSnakeCase extends CharDelimitedNamingConvention {

        public UpperSnakeCase() {
            super("_", true);
        }

    }

    /**
     * @link "https://en.wikipedia.org/wiki/Letter_case#Special_case_styles" (see Kebab-Case)
     *
     * Represents a naming convention where all letters are lower cased,
     * and words are separated by a dash sign ("-"). E.g. "my-xml-parser"
     */
    public static class LowerLispCase extends CharDelimitedNamingConvention {

        public LowerLispCase() {
            super("-", false);
        }

    }

    /**
     * @link "https://en.wikipedia.org/wiki/Letter_case#Special_case_styles" (see Kebab-Case)
     *
     * Represents a naming convention where all letters are upper cased,
     * and words are separated by a dash sign ("-"). E.g. "MY-XML-PARSER".
     */
    public static class UpperLispCase extends CharDelimitedNamingConvention {

        public UpperLispCase() {
            super("-", true);
        }

    }

    /**
     * Represents a naming convention where all letters are lower cased,
     * and words are not separated by any special character. E.g. "myxmlparser".
     */
    public static class LowerCase implements NamingConvention {

        @Override
        public List<Word> split(String input) {
            List<Word> result = new ArrayList<Word>(1);
            result.add(new Word(input));
            return result;
        }

        @Override
        public String join(List<Word> input) {
            StringBuilder builder = new StringBuilder();
            for (Word word : input) {
                builder.append(word.getValue());
            }
            return builder.toString().toLowerCase();
        }

        @Override
        public boolean equals(NamingConvention other) {
            return other != null && other instanceof LowerCase;
        }

    }

    // Abstract

    abstract public static class CamelCase implements NamingConvention {

        private static final String SPLIT_PATTERN = String.format("%s|%s|%s",
                "(?<=[A-Z])(?=[A-Z][a-z])",
                "(?<=[^A-Z])(?=[A-Z])",
                "(?<=[A-Za-z])(?=[^A-Za-z])"
        );

        private static final String ALL_UPPERCASE_PATTERN = "([A-Z])*";

        private static final Comparator<String> STRING_LENGTH_COMPARATOR = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o2.length() - o1.length();
            }
        };

        private final String ignorablePrefixPattern;

        protected CamelCase(String... ignorablePrefixes) {
            Arrays.sort(ignorablePrefixes, STRING_LENGTH_COMPARATOR);
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < ignorablePrefixes.length; i++) {
                if (i > 0) {
                    builder.append("|");
                }
                builder.append("^");
                builder.append(ignorablePrefixes[i]);
            }
            ignorablePrefixPattern = builder.toString();
        }

        @Override
        public List<Word> split(String input) {
            List<Word> result = new LinkedList<Word>();
            // slice all ignorable prefixes, then split
            for (String value : input.replaceAll(ignorablePrefixPattern, "").split(SPLIT_PATTERN)) {
                // if all uppercase, mark as abbreviation (e.g. MyXMLParser)
                boolean isAbbreviation = value.matches(ALL_UPPERCASE_PATTERN);
                Word word = new Word(value, isAbbreviation);
                result.add(word);
            }
            return result;
        }

        @Override
        public boolean equals(NamingConvention other) {
            return other instanceof CamelCase &&
                    ((CamelCase)other).ignorablePrefixPattern.equals(ignorablePrefixPattern);
        }

    }

    abstract public static class CharDelimitedNamingConvention implements NamingConvention {

        private final String delimiter;

        private final boolean isUpperCase;

        protected CharDelimitedNamingConvention(String delimiter, boolean isUpperCase) {
            this.delimiter = delimiter;
            this.isUpperCase = isUpperCase;
        }

        @Override
        public List<Word> split(String input) {
            List<Word> result = new LinkedList<Word>();
            for (String value : input.split(delimiter)) {
                result.add(new Word(value));
            }
            return result;
        }

        @Override
        public String join(List<Word> input) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < input.size(); i++) {
                if (i > 0) {
                    builder.append(delimiter);
                }
                builder.append(input.get(i).getValue());
            }
            String result = builder.toString();
            return isUpperCase ? result.toUpperCase() : result.toLowerCase();
        }

        @Override
        public boolean equals(NamingConvention other) {
            return other != null && getClass().equals(other.getClass());
        }

    }

}
