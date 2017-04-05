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
package com.datastax.driver.core.utils;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UUIDsTest {

    @Test(groups = "unit")
    public void conformanceTest() {

        // The UUIDs class does some computation at class initialization, which
        // may screw up our assumption below that UUIDs.timeBased() takes less
        // than 10ms, so force class loading now.
        UUIDs.random();

        long now = System.currentTimeMillis();
        UUID uuid = UUIDs.timeBased();

        assertEquals(uuid.version(), 1);
        assertEquals(uuid.variant(), 2);

        long tstamp = UUIDs.unixTimestamp(uuid);

        // Check now and the uuid timestamp are within 10 millisseconds.
        assert now <= tstamp && now >= tstamp - 10 : String.format("now = %d, tstamp = %d", now, tstamp);
    }

    @Test(groups = "unit")
    public void uniquenessTest() {
        // Generate 1M uuid and check we never have twice the same one

        int nbGenerated = 1000000;
        Set<UUID> generated = new HashSet<UUID>(nbGenerated);

        for (int i = 0; i < nbGenerated; ++i)
            generated.add(UUIDs.timeBased());

        assertEquals(generated.size(), nbGenerated);
    }

    @Test(groups = "unit")
    public void multiThreadUniquenessTest() throws Exception {
        int nbThread = 10;
        int nbGenerated = 10000;
        Set<UUID> generated = new ConcurrentSkipListSet<UUID>();

        UUIDGenerator[] generators = new UUIDGenerator[nbThread];
        for (int i = 0; i < nbThread; i++)
            generators[i] = new UUIDGenerator(nbGenerated, generated);

        for (int i = 0; i < nbThread; i++)
            generators[i].start();

        for (int i = 0; i < nbThread; i++)
            generators[i].join();

        assertEquals(generated.size(), nbThread * nbGenerated);
    }

    @Test(groups = "unit")
    public void timestampIncreasingTest() {
        // Generate 1M uuid and check timestamp are always increasing
        int nbGenerated = 1000000;
        long previous = 0;

        for (int i = 0; i < nbGenerated; ++i) {
            long current = UUIDs.timeBased().timestamp();
            assert previous < current : String.format("previous = %d >= %d = current", previous, current);
        }
    }

    @Test(groups = "unit")
    public void startEndOfTest() {

        Random random = new Random(System.currentTimeMillis());

        int nbTstamp = 10;
        int nbPerTstamp = 10;

        for (int i = 0; i < nbTstamp; i++) {
            long tstamp = (long) random.nextInt();
            for (int j = 0; j < nbPerTstamp; j++) {
                assertWithin(new UUID(UUIDs.makeMSB(UUIDs.fromUnixTimestamp(tstamp)), random.nextLong()), UUIDs.startOf(tstamp), UUIDs.endOf(tstamp));
            }
        }
    }

    private static void assertWithin(UUID uuid, UUID lowerBound, UUID upperBound) {
        ByteBuffer uuidBytes = TypeCodec.uuid().serialize(uuid, ProtocolVersion.V1);
        ByteBuffer lb = TypeCodec.uuid().serialize(lowerBound, ProtocolVersion.V1);
        ByteBuffer ub = TypeCodec.uuid().serialize(upperBound, ProtocolVersion.V1);
        assertTrue(compareTimestampBytes(lb, uuidBytes) <= 0);
        assertTrue(compareTimestampBytes(ub, uuidBytes) >= 0);
    }

    private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2) {
        int o1Pos = o1.position();
        int o2Pos = o2.position();

        int d = (o1.get(o1Pos + 6) & 0xF) - (o2.get(o2Pos + 6) & 0xF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 7) & 0xFF) - (o2.get(o2Pos + 7) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 4) & 0xFF) - (o2.get(o2Pos + 4) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 5) & 0xFF) - (o2.get(o2Pos + 5) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 1) & 0xFF) - (o2.get(o2Pos + 1) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 2) & 0xFF) - (o2.get(o2Pos + 2) & 0xFF);
        if (d != 0) return d;

        return (o1.get(o1Pos + 3) & 0xFF) - (o2.get(o2Pos + 3) & 0xFF);
    }

    private static class UUIDGenerator extends Thread {

        private final int toGenerate;
        private final Set<UUID> generated;

        UUIDGenerator(int toGenerate, Set<UUID> generated) {
            this.toGenerate = toGenerate;
            this.generated = generated;
        }

        @Override
        public void run() {
            for (int i = 0; i < toGenerate; ++i)
                generated.add(UUIDs.timeBased());
        }
    }
}
