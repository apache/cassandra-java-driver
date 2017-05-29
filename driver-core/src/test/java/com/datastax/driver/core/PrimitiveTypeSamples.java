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
package com.datastax.driver.core;

import com.datastax.driver.core.utils.Bytes;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This class provides sample values for each primitive data type.
 * <p/>
 * These values have no particular meaning, the goal is just to have an instance that can be used in automated tests.
 */
public class PrimitiveTypeSamples {

    static Map<DataType, Object> samples(ProtocolVersion protocolVersion) {
        try {
            final Collection<DataType> primitiveTypes = TestUtils.allPrimitiveTypes(protocolVersion);
            ImmutableMap<DataType, Object> data = ImmutableMap.<DataType, Object>builder()
                    .put(DataType.ascii(), "ascii")
                    .put(DataType.bigint(), Long.MAX_VALUE)
                    .put(DataType.blob(), Bytes.fromHexString("0xCAFE"))
                    .put(DataType.cboolean(), Boolean.TRUE)
                    .put(DataType.decimal(), new BigDecimal("12.3E+7"))
                    .put(DataType.cdouble(), Double.MAX_VALUE)
                    .put(DataType.cfloat(), Float.MAX_VALUE)
                    .put(DataType.inet(), InetAddress.getByName("123.123.123.123"))
                    .put(DataType.tinyint(), Byte.MAX_VALUE)
                    .put(DataType.smallint(), Short.MAX_VALUE)
                    .put(DataType.cint(), Integer.MAX_VALUE)
                    .put(DataType.duration(), Duration.from("PT30H20M"))
                    .put(DataType.text(), "text")
                    .put(DataType.timestamp(), new Date(872835240000L))
                    .put(DataType.date(), LocalDate.fromDaysSinceEpoch(16071))
                    .put(DataType.time(), 54012123450000L)
                    .put(DataType.timeuuid(), UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66"))
                    .put(DataType.uuid(), UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00"))
                    .put(DataType.varint(), new BigInteger(Integer.toString(Integer.MAX_VALUE) + "000"))
                    .build();

            // Only include data types that support the desired protocol version.
            Map<DataType, Object> result = Maps.filterKeys(data, new Predicate<DataType>() {
                @Override
                public boolean apply(DataType input) {
                    return primitiveTypes.contains(input);
                }
            });

            // Check that we cover all types (except counter and duration)
            // Duration is excluded because it can't be used in collections and udts.   It is tested separately
            // in DurationIntegrationTest.
            List<DataType> tmp = Lists.newArrayList(primitiveTypes);
            tmp.removeAll(result.keySet());

            List<DataType> expectedFilteredTypes = Lists.newArrayList(DataType.counter());

            assertThat(tmp)
                    .as("new datatype not covered in test")
                    .containsOnlyElementsOf(expectedFilteredTypes);

            return result;
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
}
