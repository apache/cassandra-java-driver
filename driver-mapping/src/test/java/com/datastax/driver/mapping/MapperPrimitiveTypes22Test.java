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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;

@CassandraVersion("2.2.0")
public class MapperPrimitiveTypes22Test extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE primitiveTypes22 ("
                + "byteBufferCol blob primary key,"
                + "localDateCol date,"
                + "timeCol time, timeWrapperCol time,"
                + "byteCol tinyint, byteWrapperCol tinyint,"
                + "shortCol smallint, shortWrapperCol smallint)");
    }

    @Test(groups = "short")
    public void testWriteRead22() throws Exception {
        ByteBuffer byteBufferCol = Bytes.fromHexString("0xCAFEBABE");
        LocalDate localDateCol = LocalDate.fromMillisSinceEpoch(System.currentTimeMillis());
        long timeCol = 123456789L;
        Long timeWrapperCol = 123456789L;
        byte byteCol = 42;
        Byte byteWrapperCol = 42;
        short shortCol = 16384;
        Short shortWrapperCol = 16384;

        PrimitiveTypes22 primitiveTypes = new PrimitiveTypes22();
        primitiveTypes.setByteBufferCol(byteBufferCol);
        primitiveTypes.setLocalDateCol(localDateCol);
        primitiveTypes.setTimeCol(timeCol);
        primitiveTypes.setTimeWrapperCol(timeWrapperCol);
        primitiveTypes.setByteCol(byteCol);
        primitiveTypes.setByteWrapperCol(byteWrapperCol);
        primitiveTypes.setShortCol(shortCol);
        primitiveTypes.setShortWrapperCol(shortWrapperCol);

        Mapper<PrimitiveTypes22> mapper = new MappingManager(session()).mapper(PrimitiveTypes22.class);
        mapper.save(primitiveTypes);
        PrimitiveTypes22 primitiveTypes2 = mapper.get(byteBufferCol);

        assertEquals(primitiveTypes2.getByteBufferCol(), byteBufferCol);
        assertEquals(primitiveTypes2.getLocalDateCol(), localDateCol);
        assertEquals(primitiveTypes2.getTimeCol(), timeCol);
        assertEquals(primitiveTypes2.getTimeWrapperCol(), timeWrapperCol);
        assertEquals(primitiveTypes2.getByteCol(), byteCol);
        assertEquals(primitiveTypes2.getByteWrapperCol(), byteWrapperCol);
        assertEquals(primitiveTypes2.getShortCol(), shortCol);
        assertEquals(primitiveTypes2.getShortWrapperCol(), shortWrapperCol);
    }


    @Table(name = "primitiveTypes22")
    public static class PrimitiveTypes22 {
        @PartitionKey
        private ByteBuffer byteBufferCol;
        private LocalDate localDateCol;
        private long timeCol;
        private Long timeWrapperCol;
        private byte byteCol;
        private Byte byteWrapperCol;
        private short shortCol;
        private Short shortWrapperCol;

        public ByteBuffer getByteBufferCol() {
            return byteBufferCol;
        }

        public void setByteBufferCol(ByteBuffer byteBufferCol) {
            this.byteBufferCol = byteBufferCol;
        }

        public LocalDate getLocalDateCol() {
            return localDateCol;
        }

        public void setLocalDateCol(LocalDate localDateCol) {
            this.localDateCol = localDateCol;
        }

        public long getTimeCol() {
            return timeCol;
        }

        public void setTimeCol(long timeCol) {
            this.timeCol = timeCol;
        }

        public Long getTimeWrapperCol() {
            return timeWrapperCol;
        }

        public void setTimeWrapperCol(Long timeWrapperCol) {
            this.timeWrapperCol = timeWrapperCol;
        }

        public byte getByteCol() {
            return byteCol;
        }

        public void setByteCol(byte byteCol) {
            this.byteCol = byteCol;
        }

        public Byte getByteWrapperCol() {
            return byteWrapperCol;
        }

        public void setByteWrapperCol(Byte byteWrapperCol) {
            this.byteWrapperCol = byteWrapperCol;
        }

        public short getShortCol() {
            return shortCol;
        }

        public void setShortCol(short shortCol) {
            this.shortCol = shortCol;
        }

        public Short getShortWrapperCol() {
            return shortWrapperCol;
        }

        public void setShortWrapperCol(Short shortWrapperCol) {
            this.shortWrapperCol = shortWrapperCol;
        }
    }
}
