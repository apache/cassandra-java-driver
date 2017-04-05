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
package com.datastax.driver.extras.codecs.jdk8;

import com.datastax.driver.core.Assertions;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static java.time.ZonedDateTime.parse;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("Since15")
public class ZonedDateTimeCodecTest {

    private final TupleType tupleType = TupleType.of(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE, timestamp(), varchar());

    @DataProvider(name = "ZonedDateTimeCodecTest.parse")
    public Object[][] parseParameters() {
        return new Object[][]{
                //@formatter:off
                {null                                                 , null},
                {""                                                   , null},
                {"NULL"                                               , null},
                // timestamps as milliseconds since the Epoch, offsets without zone id
                {"(0                         ,'+00:00')"              , parse("1970-01-01T00:00:00.000+00:00")},
                {"(0                         ,'+01:00')"              , parse("1970-01-01T01:00:00.000+01:00")},
                {"(1277860847999             ,'+01:00')"              , parse("2010-06-30T02:20:47.999+01:00")},
                // timestamps as valid CQL literals with different precisions, offsets without zone id
                {"('2010-06-30T01:20Z'       ,'+01:00')"              , parse("2010-06-30T02:20:00.000+01:00")},
                {"('2010-06-30T01:20:47Z'    ,'+01:00')"              , parse("2010-06-30T02:20:47.000+01:00")},
                {"('2010-06-30T01:20:47.999Z','+01:00')"              , parse("2010-06-30T02:20:47.999+01:00")},
                // zone ids with different precisions
                {"('2016-04-06T19:01Z'       ,'Z')"                   , parse("2016-04-06T19:01:00.000+00:00[Z]")},
                {"('2016-04-06T19:01Z'       ,'UTC')"                 , parse("2016-04-06T19:01:00.000+00:00[UTC]")},
                {"('2016-04-06T19:01Z'       ,'GMT')"                 , parse("2016-04-06T19:01:00.000+00:00[GMT]")},
                {"('2016-04-06T19:01Z'       ,'Etc/GMT')"             , parse("2016-04-06T19:01:00.000+00:00[Etc/GMT]")},
                {"('2016-04-06T19:01Z'       ,'Asia/Vientiane')"      , parse("2016-04-07T02:01:00.000+07:00[Asia/Vientiane]")},
                {"('2016-04-06T19:01:32Z'    ,'Asia/Vientiane')"      , parse("2016-04-07T02:01:32.000+07:00[Asia/Vientiane]")},
                {"('2016-04-06T19:01:32.999Z','Asia/Vientiane')"      , parse("2016-04-07T02:01:32.999+07:00[Asia/Vientiane]")}
                //@formatter:on
        };
    }

    @DataProvider(name = "ZonedDateTimeCodecTest.format")
    public Object[][] formatParameters() {
        return new Object[][]{
                //@formatter:off
                {null                                           , "NULL"},
                {parse("1970-01-01T00:00Z")                     , "('1970-01-01T00:00:00Z','Z')"},
                {parse("1970-01-01T00:00:00Z")                  , "('1970-01-01T00:00:00Z','Z')"},
                {parse("1970-01-01T00:00:00.000Z")              , "('1970-01-01T00:00:00Z','Z')"},
                {parse("2010-06-30T01:20+01:00")                , "('2010-06-30T00:20:00Z','+01:00')"},
                {parse("2010-06-30T01:20:47+01:00")             , "('2010-06-30T00:20:47Z','+01:00')"},
                {parse("2010-06-30T01:20:47+01:00")             , "('2010-06-30T00:20:47Z','+01:00')"},
                {parse("2010-06-30T01:20:47.999+01:00")         , "('2010-06-30T00:20:47.999Z','+01:00')"},
                {parse("2010-06-30T01:20:47.999+00:00[UTC]")    , "('2010-06-30T01:20:47.999Z','UTC')"},
                {parse("2010-06-30T01:20:47.999+00:00[GMT]")    , "('2010-06-30T01:20:47.999Z','GMT')"},
                {parse("2010-06-30T01:20:47.999+00:00[Etc/GMT]"), "('2010-06-30T01:20:47.999Z','Etc/GMT')"},
                {parse("2016-04-07T02:01+07:00[Asia/Vientiane]"), "('2016-04-06T19:01:00Z','Asia/Vientiane')"}
                //@formatter:on
        };
    }

    @Test(groups = "unit", dataProvider = "ZonedDateTimeCodecTest.parse")
    public void should_parse_valid_formats(String input, ZonedDateTime expected) {
        // given
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(tupleType);
        // when
        ZonedDateTime actual = codec.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "ZonedDateTimeCodecTest.format")
    public void should_serialize_and_format_valid_object(ZonedDateTime input, String expected) {
        // given
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(tupleType);
        // when
        String actual = codec.format(input);
        // then
        Assertions.assertThat(codec).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }

}
