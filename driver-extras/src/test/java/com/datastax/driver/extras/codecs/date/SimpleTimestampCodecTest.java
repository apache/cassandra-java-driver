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
package com.datastax.driver.extras.codecs.date;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.ParseException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ParseUtils.parseDate;
import static com.datastax.driver.core.ProtocolVersion.V4;

public class SimpleTimestampCodecTest {

    @DataProvider(name = "SimpleTimestampCodecTest.parse")
    public Object[][] parseParameters() throws ParseException {
        return new Object[][]{
                {null, null},
                {"", null},
                {"NULL", null},
                {"0", 0L},
                {"'2010-06-30T01:20'", parseDate("2010-06-30T01:20:00.000Z").getTime()},
                {"'2010-06-30T01:20Z'", parseDate("2010-06-30T01:20:00.000Z").getTime()},
                {"'2010-06-30T01:20:47'", parseDate("2010-06-30T01:20:47.000Z").getTime()},
                {"'2010-06-30T01:20:47+01:00'", parseDate("2010-06-30T00:20:47.000Z").getTime()},
                {"'2010-06-30T01:20:47.999'", parseDate("2010-06-30T01:20:47.999Z").getTime()},
                {"'2010-06-30T01:20:47.999+01:00'", parseDate("2010-06-30T00:20:47.999Z").getTime()}
        };
    }

    @DataProvider(name = "SimpleTimestampCodecTest.format")
    public Object[][] formatParameters() throws ParseException {
        return new Object[][]{
                {null, "NULL"},
                {0L, "'0'"},
                {123456L, "'123456'"}
        };
    }

    @Test(groups = "unit", dataProvider = "SimpleTimestampCodecTest.parse")
    public void should_parse_valid_formats(String input, Long expected) {
        // when
        Long actual = SimpleTimestampCodec.instance.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "SimpleTimestampCodecTest.format")
    public void should_serialize_and_format_valid_object(Long input, String expected) {
        // when
        String actual = SimpleTimestampCodec.instance.format(input);
        // then
        assertThat(SimpleTimestampCodec.instance).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }

}
