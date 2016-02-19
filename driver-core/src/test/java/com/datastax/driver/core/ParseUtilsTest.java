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
package com.datastax.driver.core;

import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

public class ParseUtilsTest {

    @Test(groups = "unit")
    public void testQuote() {
        assertThat(ParseUtils.quote(null)).isEqualTo("''");
        assertThat(ParseUtils.quote("")).isEqualTo("''");
        assertThat(ParseUtils.quote(" ")).isEqualTo("' '");
        assertThat(ParseUtils.quote("foo")).isEqualTo("'foo'");
        assertThat(ParseUtils.quote(" 'foo' ")).isEqualTo("' ''foo'' '");
    }

    @Test(groups = "unit")
    public void testUnquote() {
        assertThat(ParseUtils.unquote(null)).isNull();
        assertThat(ParseUtils.unquote("")).isEqualTo("");
        assertThat(ParseUtils.unquote(" ")).isEqualTo(" ");
        assertThat(ParseUtils.unquote("'")).isEqualTo("'"); // malformed string left untouched
        assertThat(ParseUtils.unquote("foo")).isEqualTo("foo");
        assertThat(ParseUtils.unquote("''")).isEqualTo("");
        assertThat(ParseUtils.unquote("' '")).isEqualTo(" ");
        assertThat(ParseUtils.unquote("'foo")).isEqualTo("'foo"); // malformed string left untouched
        assertThat(ParseUtils.unquote("'foo'")).isEqualTo("foo");
        assertThat(ParseUtils.unquote(" 'foo' ")).isEqualTo(" 'foo' "); // considered unquoted
        assertThat(ParseUtils.unquote("'''foo'''")).isEqualTo("'foo'");
    }

    @Test(groups = "unit")
    public void testIsQuoted() {
        assertThat(ParseUtils.isQuoted(null)).isFalse();
        assertThat(ParseUtils.isQuoted("")).isFalse();
        assertThat(ParseUtils.isQuoted(" ")).isFalse();
        assertThat(ParseUtils.isQuoted("'")).isFalse(); // malformed string considered unquoted
        assertThat(ParseUtils.isQuoted("foo")).isFalse();
        assertThat(ParseUtils.isQuoted("''")).isTrue();
        assertThat(ParseUtils.isQuoted("' '")).isTrue();
        assertThat(ParseUtils.isQuoted("'foo")).isFalse(); // malformed string considered unquoted
        assertThat(ParseUtils.isQuoted("'foo'")).isTrue();
        assertThat(ParseUtils.isQuoted(" 'foo' ")).isFalse(); // considered unquoted
        assertThat(ParseUtils.isQuoted("'''foo'''")).isTrue();
    }

}
