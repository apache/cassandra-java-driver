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
import static com.datastax.driver.core.CodecUtils.fromCqlDateToDaysSinceEpoch;
import static com.datastax.driver.core.ParseUtils.parseDate;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SimpleDateCodecTest {

    @DataProvider(name = "SimpleDateCodecTest.parse")
    public Object[][] parseParameters() throws ParseException {
        int _2014_01_01 = (int) MILLISECONDS.toDays(parseDate("2014-01-01", "yyyy-MM-dd").getTime());
        return new Object[][]{
                {null, null},
                {"", null},
                {"NULL", null},
                {"0", fromCqlDateToDaysSinceEpoch(0)},
                {"'2147483648'", 0},
                // SimpleDateFormat is unable to parse year -5877641
                //{ "'-5877641-06-23'"  , fromCqlDateToDaysSinceEpoch(0) },
                {"'1970-01-01'", 0},
                {"'2014-01-01'", _2014_01_01}
        };
    }

    @DataProvider(name = "SimpleDateCodecTest.format")
    public Object[][] formatParameters() throws ParseException {
        int _2014_01_01 = (int) MILLISECONDS.toDays(parseDate("2014-01-01", "yyyy-MM-dd").getTime());
        return new Object[][]{
                {null, "NULL"},
                {fromCqlDateToDaysSinceEpoch(0), "'0'"},
                {0, "'2147483648'"},
                {_2014_01_01, "'2147499719'"}
        };
    }

    @Test(groups = "unit", dataProvider = "SimpleDateCodecTest.parse")
    public void should_parse_valid_formats(String input, Integer expected) {
        // when
        Integer actual = SimpleDateCodec.instance.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "SimpleDateCodecTest.format")
    public void should_serialize_and_format_valid_object(Integer input, String expected) {
        // when
        String actual = SimpleDateCodec.instance.format(input);
        // then
        assertThat(SimpleDateCodec.instance).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }

}
