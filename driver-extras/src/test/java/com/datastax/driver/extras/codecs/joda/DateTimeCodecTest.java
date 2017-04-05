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
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TupleType;
import org.joda.time.DateTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.forID;

public class DateTimeCodecTest {

    private final TupleType tupleType = TupleType.of(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE, timestamp(), varchar());

    @DataProvider(name = "DateTimeCodecTest.parse")
    public Object[][] parseParameters() {
        return new Object[][]{
                //@formatter:off
                {null                                               , null},
                {""                                                 , null},
                {"NULL"                                             , null},
                // timestamps as milliseconds since the Epoch, offsets without zone id
                {"(0                         ,'+00:00')"            , new DateTime("1970-01-01T00:00:00.000+00:00", forID("+00:00"))},
                {"(0                         ,'+01:00')"            , new DateTime("1970-01-01T01:00:00.000+01:00", forID("+01:00"))},
                {"(1277860847999             ,'+01:00')"            , new DateTime("2010-06-30T02:20:47.999+01:00", forID("+01:00"))},
                // timestamps as valid CQL literals with different precisions, offsets without zone id
                {"('2010-06-30T01:20Z'       ,'+01:00')"            , new DateTime("2010-06-30T02:20:00.000+01:00", forID("+01:00"))},
                {"('2010-06-30T01:20:47Z'    ,'+01:00')"            , new DateTime("2010-06-30T02:20:47.000+01:00", forID("+01:00"))},
                {"('2010-06-30T01:20:47.999Z','+01:00')"            , new DateTime("2010-06-30T02:20:47.999+01:00", forID("+01:00"))},
                // zone ids with different precisions
                {"('2016-04-06T19:01Z'       ,'Z')"                 , new DateTime("2016-04-06T19:01:00.000+00:00", forID("UTC"))},
                {"('2016-04-06T19:01Z'       ,'UTC')"               , new DateTime("2016-04-06T19:01:00.000+00:00", forID("UTC"))},
                {"('2016-04-06T19:01Z'       ,'GMT')"               , new DateTime("2016-04-06T19:01:00.000+00:00", forID("GMT"))},
                {"('2016-04-06T19:01Z'       ,'Etc/GMT')"           , new DateTime("2016-04-06T19:01:00.000+00:00", forID("GMT"))},
                {"('2016-04-06T19:01Z'       ,'Asia/Vientiane')"    , new DateTime("2016-04-07T02:01:00.000+07:00", forID("Asia/Vientiane"))},
                {"('2016-04-06T19:01:32Z'    ,'Asia/Vientiane')"    , new DateTime("2016-04-07T02:01:32.000+07:00", forID("Asia/Vientiane"))},
                {"('2016-04-06T19:01:32.999Z','Asia/Vientiane')"    , new DateTime("2016-04-07T02:01:32.999+07:00", forID("Asia/Vientiane"))},
                //@formatter:on
        };
    }

    @DataProvider(name = "DateTimeCodecTest.format")
    public Object[][] formatParameters() {
        return new Object[][]{
                {null, "NULL"},
                //@formatter:off
                {new DateTime("1970-01-01T00:00Z"             , forID("+00:00"))        , "('1970-01-01T00:00:00.000Z','UTC')"},
                {new DateTime("1970-01-01T00:00:00Z"          , forID("+00:00"))        , "('1970-01-01T00:00:00.000Z','UTC')"},
                {new DateTime("1970-01-01T00:00:00.000Z"      , forID("+00:00"))        , "('1970-01-01T00:00:00.000Z','UTC')"},
                {new DateTime("2010-06-30T01:20+01:00"        , forID("+01:00"))        , "('2010-06-30T00:20:00.000Z','+01:00')"},
                {new DateTime("2010-06-30T01:20:47+01:00"     , forID("+01:00"))        , "('2010-06-30T00:20:47.000Z','+01:00')"},
                {new DateTime("2010-06-30T01:20:47.999+01:00" , forID("+01:00"))        , "('2010-06-30T00:20:47.999Z','+01:00')"},
                {new DateTime("2016-04-07T02:01Z"             , forID("UTC"))           , "('2016-04-07T02:01:00.000Z','UTC')"},
                {new DateTime("2016-04-07T02:01Z"             , forID("GMT"))           , "('2016-04-07T02:01:00.000Z','Etc/GMT')"},
                {new DateTime("2016-04-07T02:01+07:00"        , forID("Asia/Vientiane")), "('2016-04-06T19:01:00.000Z','Asia/Vientiane')"}
                //@formatter:on
        };
    }

    @Test(groups = "unit", dataProvider = "DateTimeCodecTest.parse")
    public void should_parse_valid_formats(String input, DateTime expected) {
        // given
        DateTimeCodec codec = new DateTimeCodec(tupleType);
        // when
        DateTime actual = codec.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit", dataProvider = "DateTimeCodecTest.format")
    public void should_serialize_and_format_valid_object(DateTime input, String expected) {
        // given
        DateTimeCodec codec = new DateTimeCodec(tupleType);
        // when
        String actual = codec.format(input);
        // then
        Assertions.assertThat(codec).withProtocolVersion(V4).canSerialize(input);
        assertThat(actual).isEqualTo(expected);
    }

}
