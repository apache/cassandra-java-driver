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
package com.datastax.driver.extras.codecs.joda;

import com.datastax.driver.core.Assertions;
import org.joda.time.LocalDate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalDateCodecTest {

    private static final LocalDate EPOCH = new LocalDate(1970, 1, 1);
    private static final LocalDate MIN = LocalDate.parse("-5877641-06-23");

    @DataProvider(name = "LocalDateCodecTest")
    public Object[][] parseParameters() {
        return new Object[][]{
                {null, null},
                {"", null},
                {"NULL", null},
                {"0", MIN},
                {"'2147483648'", EPOCH},
                {"'-5877641-06-23'", MIN},
                {"'1970-01-01'", EPOCH},
                {"'2014-01-01'", LocalDate.parse("2014-01-01")}
        };
    }

    @DataProvider(name = "LocalDateCodecTest.format")
    public Object[][] formatParameters() {
        return new Object[][]{
                {null, "NULL"},
                {EPOCH, "'1970-01-01'"},
                {LocalDate.parse("2010-06-30"), "'2010-06-30'"}
        };
    }

    @Test(groups = "unit", dataProvider = "LocalDateCodecTest")
    public void should_parse_valid_formats(String input, LocalDate expected) {
        // when
        LocalDate actual = LocalDateCodec.instance.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "LocalDateCodecTest.format")
    public void should_serialize_and_format_valid_object(LocalDate input, String expected) {
        // when
        String actual = LocalDateCodec.instance.format(input);
        // then
        Assertions.assertThat(LocalDateCodec.instance).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }

}
