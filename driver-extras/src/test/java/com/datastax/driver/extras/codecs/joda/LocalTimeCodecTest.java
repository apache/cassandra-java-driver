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
import org.joda.time.LocalTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ParseUtils.quote;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalTimeCodecTest {

    @DataProvider(name = "LocalTimeCodecTest.parse")
    public Object[][] parseParameters() {
        LocalTime time = LocalTime.parse("13:25:47.123456789");
        String nanosOfDay = quote(Long.toString(MILLISECONDS.toNanos(time.getMillisOfDay())));
        return new Object[][]{
                {null, null},
                {"", null},
                {"NULL", null},
                {nanosOfDay, LocalTime.parse("13:25:47.123456789")},
                {"'13:25:47'", LocalTime.parse("13:25:47")},
                {"'13:25:47.123'", LocalTime.parse("13:25:47.123")},
                {"'13:25:47.123456'", LocalTime.parse("13:25:47.123456")},
                {"'13:25:47.123456789'", LocalTime.parse("13:25:47.123456789")}
        };
    }

    @DataProvider(name = "LocalTimeCodecTest.format")
    public Object[][] formatParameters() {
        return new Object[][]{
                {null, "NULL"},
                {LocalTime.parse("12:00"), "'12:00:00.000'"},
                {LocalTime.parse("02:20:47.999999"), "'02:20:47.999'"}
        };
    }

    @Test(groups = "unit", dataProvider = "LocalTimeCodecTest.parse")
    public void should_parse_valid_formats(String input, LocalTime expected) {
        // when
        LocalTime actual = LocalTimeCodec.instance.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "LocalTimeCodecTest.format")
    public void should_serialize_and_format_valid_object(LocalTime input, String expected) {
        // when
        String actual = LocalTimeCodec.instance.format(input);
        // then
        Assertions.assertThat(LocalTimeCodec.instance).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }


}
