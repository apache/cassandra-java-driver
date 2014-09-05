package com.datastax.driver.mapping;

import java.util.ArrayList;
import java.util.List;

import org.testng.util.Strings;

import com.datastax.driver.mapping.annotations.Frozen;

/** A CQL type definition parsed from {@link Frozen#value()}, for example map<text,map<text,frozen<user>>> */
class CQLType {
    String name;
    boolean frozen;
    List<CQLType> subTypes;

    private CQLType(String name) {
        this(name, false);
    }

    private CQLType(String name, boolean frozen, CQLType... subTypes) {
        this.name = name;
        this.frozen = frozen;
        for (CQLType subType : subTypes) {
            if (this.subTypes == null)
                this.subTypes = new ArrayList<CQLType>();
            this.subTypes.add(subType);
        }
    }

    // Some constant types for the simple cases. Note that we can afford generic names because currently we don't include the name in our
    // checks.
    static final CQLType FROZEN_SIMPLE = new CQLType("root", true);
    static final CQLType FROZEN_ELEMENT = new CQLType("collection", false,
                                                      new CQLType("element", true));
    static final CQLType FROZEN_MAP_KEY = new CQLType("map", false,
                                                      new CQLType("key", true));
    static final CQLType FROZEN_MAP_VALUE = new CQLType("map", false,
                                                        new CQLType("key", false),
                                                        new CQLType("value", true));
    static final CQLType FROZEN_MAP_KEY_AND_VALUE = new CQLType("map", false,
                                                                new CQLType("key", true),
                                                                new CQLType("value", true));
    static final CQLType UNFROZEN_SIMPLE = new CQLType("root");

    static CQLType parse(String toParse) {
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

        CQLType parse() {
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
                return new CQLType(name);
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
                    CQLType type = parse();
                    skipSpaces();
                    if (idx >= toParse.length() || toParse.charAt(idx) != '>')
                        throw fail("expected '>'");
                    idx += 1;
                    type.frozen = true;
                    return type;
                } else {
                    CQLType type = new CQLType(nextWord);
                    skipSpaces();
                    if (idx < toParse.length() && toParse.charAt(idx) == '<') {
                        idx += 1;
                        type.subTypes = new ArrayList<CQLType>();
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

            // TODO check allowed characters in unquoted names
            if (!isAlpha(idx))
                throw fail("illegal character at start of type name");

            int i = idx;
            while (i < toParse.length() && isAlphaNum(i))
                i += 1;

            return i;
        }

        private boolean isBlank(int i) {
            char c = toParse.charAt(i);
            return c == ' ' || c == '\t' || c == '\n';
        }

        private boolean isAlpha(int i) {
            char c = toParse.charAt(i);
            return (c >= 'a' && c <= 'z') ||
                   (c >= 'A' && c <= 'Z');
        }

        private boolean isAlphaNum(int i) {
            if (isAlpha(i))
                return true;
            char c = toParse.charAt(i);
            return c >= '0' && c <= '9';
        }

        private IllegalArgumentException fail(String cause) {
            return new IllegalArgumentException(cause + " (" + toParse + " [" + idx + "])");
        }
    }
}
