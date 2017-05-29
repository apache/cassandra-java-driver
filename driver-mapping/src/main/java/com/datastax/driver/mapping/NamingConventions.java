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

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Implementations of industry common naming conventions.
 */
public class NamingConventions {

    /**
     * Represents a naming convention where all letters are lower cased,
     * and words are not separated by any special character. E.g. "myxmlparser".
     */
    public static final NamingConvention LOWER_CASE = new SingleWordNamingConvention(false);

    /**
     * Represents a naming convention where all letters are upper cased,
     * and words are not separated by any special character. E.g. "MYXMLPARSER".
     */
    public static final NamingConvention UPPER_CASE = new SingleWordNamingConvention(true);

    /**
     * Represents <a href="https://en.wikipedia.org/wiki/Snake_case">snake case</a> naming convention, meaning all letters are lower cased,
     * and words are separated by an underscore ("_"). E.g. "my_xml_parser".
     */
    public static final NamingConvention LOWER_SNAKE_CASE = new CharDelimitedNamingConvention("_", false);

    /**
     * Represents <a href="https://en.wikipedia.org/wiki/Snake_case">snake case</a> naming convention, meaning all letters are upper cased,
     * and words are separated by an underscore ("_"). E.g. "MY_XML_PARSER".
     */
    public static final NamingConvention UPPER_SNAKE_CASE = new CharDelimitedNamingConvention("_", true);

    /**
     * Represents <a href="https://en.wikipedia.org/wiki/Letter_case#Special_case_styles">Lisp case</a> naming convention,
     * meaning all letters are lower cased,
     * and words are separated by a dash sign ("-"). E.g. "my-xml-parser"
     */
    public static final NamingConvention LOWER_LISP_CASE = new CharDelimitedNamingConvention("-", false);

    /**
     * Represents <a href="https://en.wikipedia.org/wiki/Letter_case#Special_case_styles">Lisp case</a> naming convention,
     * meaning all letters are upper cased,
     * and words are separated by a dash sign ("-"). E.g. "MY-XML-PARSER".
     */
    public static final NamingConvention UPPER_LISP_CASE = new CharDelimitedNamingConvention("-", true);

    /**
     * Represents the default <a href="https://en.wikipedia.org/wiki/Camel_case">Camel case</a> naming convention,
     * with a lower cased first letter.
     *
     * @see LowerCamelCase
     */
    public static final NamingConvention LOWER_CAMEL_CASE = new LowerCamelCase();

    /**
     * Represents the default <a href="https://en.wikipedia.org/wiki/Camel_case">Camel case</a> naming convention,
     * with an upper cased first letter.
     *
     * @see UpperCamelCase
     */
    public static final NamingConvention UPPER_CAMEL_CASE = new UpperCamelCase();

    /**
     * Represents <a href="https://en.wikipedia.org/wiki/Camel_case">Camel case</a>
     * naming convention with a lower cased first letter.
     * <p/>
     * E.g. "myXmlParser" and "myXMLParser". Note that both examples are valid
     * lower camel case forms. The first one takes abbreviations as any other
     * words where the first letter is upper case, and the rest are lower case
     * (hence - "Xml"), while the latter upper cases all letters of an abbreviation
     * (hence - "XML").
     * <p/>
     * Additionally, many different Java naming conventions introduce prefixes
     * for field naming, some examples:
     * <ul>
     * <li><a href="http://source.android.com/source/code-style.html#follow-field-naming-conventions">Android</a></li>
     * <li><a href="https://en.wikipedia.org/wiki/Hungarian_notation">Hungarian Notation</a></li>
     * <li><a href="http://stackoverflow.com/questions/1899683/is-there-a-standard-in-java-for-underscore-in-front-of-variable-or-class-nam">Underscore</a></li>
     * </ul>
     * Those prefixes can be supported. For example, if this convention is
     * configured with {@code ignorablePrefixes} set to "_" then a field
     * named "_myXmlParser" will be split in 3 words only: "my", "Xml", "Parser".
     */
    public static class LowerCamelCase extends CamelCase {

        private final boolean upperCaseAbbreviations;

        /**
         * @param upperCaseAbbreviations {@code true} to uppercase all abbreviations,
         *                               {@code false} to treat abbreviations as any other word
         * @param ignorablePrefixes      string prefixes to trim if constant field name prefixes are used
         */
        public LowerCamelCase(boolean upperCaseAbbreviations, String... ignorablePrefixes) {
            super(ignorablePrefixes);
            this.upperCaseAbbreviations = upperCaseAbbreviations;
        }

        /**
         * @param ignorablePrefixes string prefixes to trim if constant field name prefixes are used
         */
        public LowerCamelCase(String... ignorablePrefixes) {
            this(false, ignorablePrefixes);
        }

        /**
         * @param upperCaseAbbreviations {@code true} to uppercase all abbreviations,
         *                               {@code false} to treat abbreviations as any other word
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
                } else if (upperCaseAbbreviations && word.isAbbreviation()) {
                    value = word.getValue().toUpperCase();
                } else {
                    value = word.getValue().substring(0, 1).toUpperCase() + word.getValue().substring(1).toLowerCase();
                }
                builder.append(value);
            }
            return builder.toString();
        }

    }

    /**
     * Represents <a href="https://en.wikipedia.org/wiki/Camel_case">Camel case</a>
     * naming convention with an upper cased first letter.
     * <p/>
     * E.g. "MyXmlParser" and "MyXMLParser". Note that both examples are valid
     * upper camel case forms. The first one takes abbreviations as any other
     * words where the first letter is upper case, and the rest are lower case
     * (hence - "Xml"), while the latter upper cases all letters of an abbreviation
     * (hence - "XML").
     * <p/>
     * Additionally, many different Java naming conventions introduce prefixes
     * for field naming, some examples:
     * <ul>
     * <li><a href="http://source.android.com/source/code-style.html#follow-field-naming-conventions">Android</a></li>
     * <li><a href="https://en.wikipedia.org/wiki/Hungarian_notation">Hungarian Notation</a></li>
     * <li><a href="http://stackoverflow.com/questions/1899683/is-there-a-standard-in-java-for-underscore-in-front-of-variable-or-class-nam">Underscore</a></li>
     * </ul>
     * Those prefixes can be supported. For example, if this convention is
     * configured with {@code ignorablePrefixes} set to "_" then a field
     * named "_MyXmlParser" will be split in 3 words only: "My", "Xml", "Parser".
     */
    public static class UpperCamelCase extends CamelCase {

        private final boolean upperCaseAbbreviations;

        /**
         * @param upperCaseAbbreviations {@code true} to uppercase all abbreviations,
         *                               {@code false} to treat abbreviations as any other word
         * @param ignorablePrefixes      string prefixes to trim if constant field name prefixes are used
         */
        public UpperCamelCase(boolean upperCaseAbbreviations, String... ignorablePrefixes) {
            super(ignorablePrefixes);
            this.upperCaseAbbreviations = upperCaseAbbreviations;
        }

        /**
         * @param ignorablePrefixes string prefixes to trim if constant field name prefixes are used
         */
        public UpperCamelCase(String... ignorablePrefixes) {
            this(false, ignorablePrefixes);
        }

        /**
         * @param upperCaseAbbreviations {@code true} to uppercase all abbreviations,
         *                               {@code false} to treat abbreviations as any other word
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
                } else {
                    value = word.getValue().substring(0, 1).toUpperCase() + word.getValue().substring(1).toLowerCase();
                }
                builder.append(value);
            }
            return builder.toString();
        }

    }

    public abstract static class CamelCase implements NamingConvention {

        private static final Pattern SPLIT_PATTERN = Pattern.compile(String.format("%s|%s|%s",
                "(?<=[A-Z])(?=[A-Z][a-z])",
                "(?<=[^A-Z])(?=[A-Z])",
                "(?<=[A-Za-z])(?=[^A-Za-z])"
        ));

        private static final Pattern ALL_UPPERCASE_PATTERN = Pattern.compile("([A-Z])*");

        private static final Comparator<String> STRING_LENGTH_COMPARATOR = new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o2.length() - o1.length();
            }
        };

        private final Pattern ignorablePrefixPattern;

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
            ignorablePrefixPattern = Pattern.compile(builder.toString());
        }

        @Override
        public List<Word> split(String input) {
            List<Word> result = new LinkedList<Word>();
            // slice all ignorable prefixes, then split
            for (String value : SPLIT_PATTERN.split(ignorablePrefixPattern.matcher(input).replaceAll(""))) {
                // if all uppercase, mark as abbreviation (e.g. MyXMLParser)
                boolean isAbbreviation = ALL_UPPERCASE_PATTERN.matcher(value).matches();
                Word word = new Word(value, isAbbreviation);
                result.add(word);
            }
            return result;
        }

    }

    public static class CharDelimitedNamingConvention implements NamingConvention {

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

    }

    public static class SingleWordNamingConvention implements NamingConvention {

        private final boolean isUpperCase;

        protected SingleWordNamingConvention(boolean isUpperCase) {
            this.isUpperCase = isUpperCase;
        }

        @Override
        public List<Word> split(String input) {
            List<Word> result = new LinkedList<Word>();
            result.add(new Word(input));
            return result;
        }

        @Override
        public String join(List<Word> input) {
            StringBuilder builder = new StringBuilder();
            for (Word word : input) {
                builder.append(word.getValue());
            }
            String result = builder.toString();
            return isUpperCase ? result.toUpperCase() : result.toLowerCase();
        }

    }
}
