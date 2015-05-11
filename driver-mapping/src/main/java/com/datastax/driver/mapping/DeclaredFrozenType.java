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
package com.datastax.driver.mapping;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.mapping.annotations.Frozen;
import com.google.common.base.Strings;

/**
 * A tree-like structure parsed from {@code Frozen*} annotations, that indicates which
 * types in the hierarchy were declared frozen.
 */
class DeclaredFrozenType {
    String name;
    boolean frozen;
    List<DeclaredFrozenType> subTypes;

    private DeclaredFrozenType(String name) {
        this(name, false);
    }

    private DeclaredFrozenType(String name, boolean frozen, DeclaredFrozenType... subTypes) {
        this.name = name;
        this.frozen = frozen;
        for (DeclaredFrozenType subType : subTypes) {
            if (this.subTypes == null)
                this.subTypes = new ArrayList<DeclaredFrozenType>();
            this.subTypes.add(subType);
        }
    }

    // Some constant types for the simple cases. Note that we can afford generic names because currently we don't include the name in our
    // checks.
    static final DeclaredFrozenType FROZEN_SIMPLE = new DeclaredFrozenType("root", true);
    static final DeclaredFrozenType FROZEN_ELEMENT = new DeclaredFrozenType("collection", false,
                                                      new DeclaredFrozenType("element", true));
    static final DeclaredFrozenType FROZEN_MAP_KEY = new DeclaredFrozenType("map", false,
                                                      new DeclaredFrozenType("key", true));
    static final DeclaredFrozenType FROZEN_MAP_VALUE = new DeclaredFrozenType("map", false,
                                                        new DeclaredFrozenType("key", false),
                                                        new DeclaredFrozenType("value", true));
    static final DeclaredFrozenType FROZEN_MAP_KEY_AND_VALUE = new DeclaredFrozenType("map", false,
                                                                new DeclaredFrozenType("key", true),
                                                                new DeclaredFrozenType("value", true));
    static final DeclaredFrozenType UNFROZEN_SIMPLE = new DeclaredFrozenType("root");

    static DeclaredFrozenType parse(String toParse) {
        if (Strings.isNullOrEmpty(toParse))
            // we allow that as a shorthand
            return FROZEN_SIMPLE;
        else
            return new Parser(toParse).parse();
    }

    private static class Parser {
        private final String toParse;
        private int idx;

        Parser(String toParse) {
            this.toParse = toParse;
        }

        DeclaredFrozenType parse() {
            skipSpaces();

            if (toParse.charAt(idx) == '"') {
                // quoted identifier: just return everything up to the next quote
                int endQuote = idx + 1;
                while (endQuote < toParse.length() && toParse.charAt(endQuote) != '"')
                    endQuote += 1;
                if (endQuote == toParse.length())
                    throw fail("could not find matching quote");
                if (endQuote == idx + 1)
                    throw fail("empty quoted identifier");

                String name = toParse.substring(idx + 1, endQuote);
                idx = endQuote + 1;
                // a quoted identifier cannot be parameterized, so we can return immediately
                return new DeclaredFrozenType(name);
            } else {
                // unquoted identifier: could be "simpleType", "complexType<subType1, subType2...>" or "frozen<type>"
                int n = skipWord();
                String nextWord = toParse.substring(idx, n).toLowerCase();
                idx = n;
                if ("frozen".equals(nextWord)) {
                    skipSpaces();
                    if (idx >= toParse.length() || toParse.charAt(idx) != '<')
                        throw fail("expected '<'");
                    idx += 1;
                    DeclaredFrozenType type = parse();
                    skipSpaces();
                    if (idx >= toParse.length() || toParse.charAt(idx) != '>')
                        throw fail("expected '>'");
                    idx += 1;
                    type.frozen = true;
                    return type;
                } else {
                    DeclaredFrozenType type = new DeclaredFrozenType(nextWord);
                    skipSpaces();
                    if (idx < toParse.length() && toParse.charAt(idx) == '<') {
                        idx += 1;
                        type.subTypes = new ArrayList<DeclaredFrozenType>();
                        while (idx < toParse.length()) {
                            type.subTypes.add(parse());
                            skipSpaces();
                            if (idx >= toParse.length())
                                fail("unterminated list of subtypes");
                            else if (toParse.charAt(idx) == '>') {
                                idx += 1;
                                break;
                            }
                            else if (toParse.charAt(idx) != ',')
                                fail("expected ','");
                            else
                                idx += 1;
                        }
                    }
                    return type;
                }
            }
        }

        private void skipSpaces() {
            while (idx < toParse.length() && isBlank(idx))
                idx += 1;
        }

        private int skipWord() {
            if (idx >= toParse.length())
                throw fail("expected type name");

            if (!isIdentStart(idx))
                throw fail("illegal character at start of type name");

            int i = idx;
            while (i < toParse.length() && isIdentBody(i))
                i += 1;

            return i;
        }

        private boolean isBlank(int i) {
            char c = toParse.charAt(i);
            return c == ' ' || c == '\t' || c == '\n';
        }

        private boolean isIdentStart(int i) {
            char c = toParse.charAt(i);
            return isLetter(c);
        }

        private boolean isIdentBody(int i) {
            char c = toParse.charAt(i);
            return isLetter(c) || isDigit(c) || c == '_';
        }

        private boolean isLetter(char c) {
            return (c >= 'a' && c <= 'z') ||
                   (c >= 'A' && c <= 'Z');
        }

        private boolean isDigit(char c) {
            return (c >= '0' && c <= '9');
        }

        private IllegalArgumentException fail(String cause) {
            return new IllegalArgumentException(cause + " (" + toParse + " [" + idx + "])");
        }
    }
}
