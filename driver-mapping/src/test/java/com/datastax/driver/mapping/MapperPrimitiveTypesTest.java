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
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

/**
 * Tests the mapping of all primitive types as Java fields.
 */
public class MapperPrimitiveTypesTest extends CCMTestsSupport {

    public void onTestContextInitialized() {
        execute("CREATE TABLE primitiveTypes ("
                + "byteBufferCol blob primary key,"
                + "intCol int, intWrapperCol int,"
                + "longCol bigint, longWrapperCol bigint,"
                + "floatCol float, floatWrapperCol float,"
                + "doubleCol double, doubleWrapperCol double,"
                + "booleanCol boolean, booleanWrapperCol boolean,"
                + "bigDecimalCol decimal,"
                + "bigIntegerCol varint,"
                + "stringCol text,"
                + "inetCol inet,"
                + "dateCol timestamp,"
                + "uuidCol uuid,"
                + "timeUuidCol timeuuid)");
    }

    @Test(groups = "short")
    public void testWriteRead() throws Exception {
        ByteBuffer byteBufferCol = Bytes.fromHexString("0xCAFEBABE");
        int intCol = 1;
        Integer intWrapperCol = 1;
        long longCol = 1L;
        Long longWrapperCol = 1L;
        float floatCol = 1.0F;
        Float floatWrapperCol = 1.0F;
        double doubleCol = 1.0;
        Double doubleWrapperCol = 1.0;
        boolean booleanCol = true;
        Boolean booleanWrapperCol = Boolean.TRUE;
        BigDecimal bigDecimalCol = BigDecimal.ONE;
        BigInteger bigIntegerCol = BigInteger.ONE;
        String stringCol = "Col";
        InetAddress inetCol = InetAddress.getLocalHost();
        Date dateCol = new Date();
        UUID uuidCol = UUIDs.random();
        UUID timeUuidCol = UUIDs.timeBased();

        PrimitiveTypes primitiveTypes = new PrimitiveTypes();
        primitiveTypes.setByteBufferCol(byteBufferCol);
        primitiveTypes.setIntCol(intCol);
        primitiveTypes.setIntWrapperCol(intWrapperCol);
        primitiveTypes.setLongCol(longCol);
        primitiveTypes.setLongWrapperCol(longWrapperCol);
        primitiveTypes.setFloatCol(floatCol);
        primitiveTypes.setFloatWrapperCol(floatWrapperCol);
        primitiveTypes.setDoubleCol(doubleCol);
        primitiveTypes.setDoubleWrapperCol(doubleWrapperCol);
        primitiveTypes.setBooleanCol(booleanCol);
        primitiveTypes.setBooleanWrapperCol(booleanWrapperCol);
        primitiveTypes.setBigDecimalCol(bigDecimalCol);
        primitiveTypes.setBigIntegerCol(bigIntegerCol);
        primitiveTypes.setStringCol(stringCol);
        primitiveTypes.setInetCol(inetCol);
        primitiveTypes.setDateCol(dateCol);
        primitiveTypes.setUuidCol(uuidCol);
        primitiveTypes.setTimeUuidCol(timeUuidCol);

        Mapper<PrimitiveTypes> mapper = new MappingManager(session()).mapper(PrimitiveTypes.class);
        mapper.save(primitiveTypes);
        PrimitiveTypes primitiveTypes2 = mapper.get(byteBufferCol);

        assertEquals(primitiveTypes2.getByteBufferCol(), byteBufferCol);
        assertEquals(primitiveTypes2.getIntCol(), intCol);
        assertEquals(primitiveTypes2.getIntWrapperCol(), intWrapperCol);
        assertEquals(primitiveTypes2.getLongCol(), longCol);
        assertEquals(primitiveTypes2.getLongWrapperCol(), longWrapperCol);
        assertEquals(primitiveTypes2.getFloatCol(), floatCol);
        assertEquals(primitiveTypes2.getFloatWrapperCol(), floatWrapperCol);
        assertEquals(primitiveTypes2.getDoubleCol(), doubleCol);
        assertEquals(primitiveTypes2.getDoubleWrapperCol(), doubleWrapperCol);
        assertEquals(primitiveTypes2.isBooleanCol(), booleanCol);
        assertEquals(primitiveTypes2.getBooleanWrapperCol(), booleanWrapperCol);
        assertEquals(primitiveTypes2.getBigDecimalCol(), bigDecimalCol);
        assertEquals(primitiveTypes2.getBigIntegerCol(), bigIntegerCol);
        assertEquals(primitiveTypes2.getStringCol(), stringCol);
        assertEquals(primitiveTypes2.getInetCol(), inetCol);
        assertEquals(primitiveTypes2.getDateCol(), dateCol);
        assertEquals(primitiveTypes2.getUuidCol(), uuidCol);
        assertEquals(primitiveTypes2.getTimeUuidCol(), timeUuidCol);
    }

    @Table(name = "primitiveTypes")
    public static class PrimitiveTypes {
        @PartitionKey
        private ByteBuffer byteBufferCol;
        private int intCol;
        private Integer intWrapperCol;
        private long longCol;
        private Long longWrapperCol;
        private float floatCol;
        private Float floatWrapperCol;
        private double doubleCol;
        private Double doubleWrapperCol;
        private boolean booleanCol;
        private Boolean booleanWrapperCol;
        private BigDecimal bigDecimalCol;
        private BigInteger bigIntegerCol;
        private String stringCol;
        private InetAddress inetCol;
        private Date dateCol;
        private UUID uuidCol;
        private UUID timeUuidCol;

        public ByteBuffer getByteBufferCol() {
            return byteBufferCol;
        }

        public void setByteBufferCol(ByteBuffer byteBufferCol) {
            this.byteBufferCol = byteBufferCol;
        }

        public int getIntCol() {
            return intCol;
        }

        public void setIntCol(int intCol) {
            this.intCol = intCol;
        }

        public Integer getIntWrapperCol() {
            return intWrapperCol;
        }

        public void setIntWrapperCol(Integer intWrapperCol) {
            this.intWrapperCol = intWrapperCol;
        }

        public long getLongCol() {
            return longCol;
        }

        public void setLongCol(long longCol) {
            this.longCol = longCol;
        }

        public Long getLongWrapperCol() {
            return longWrapperCol;
        }

        public void setLongWrapperCol(Long longWrapperCol) {
            this.longWrapperCol = longWrapperCol;
        }

        public float getFloatCol() {
            return floatCol;
        }

        public void setFloatCol(float floatCol) {
            this.floatCol = floatCol;
        }

        public Float getFloatWrapperCol() {
            return floatWrapperCol;
        }

        public void setFloatWrapperCol(Float floatWrapperCol) {
            this.floatWrapperCol = floatWrapperCol;
        }

        public double getDoubleCol() {
            return doubleCol;
        }

        public void setDoubleCol(double doubleCol) {
            this.doubleCol = doubleCol;
        }

        public Double getDoubleWrapperCol() {
            return doubleWrapperCol;
        }

        public void setDoubleWrapperCol(Double doubleWrapperCol) {
            this.doubleWrapperCol = doubleWrapperCol;
        }

        public boolean isBooleanCol() {
            return booleanCol;
        }

        public void setBooleanCol(boolean booleanCol) {
            this.booleanCol = booleanCol;
        }

        public Boolean getBooleanWrapperCol() {
            return booleanWrapperCol;
        }

        public void setBooleanWrapperCol(Boolean booleanWrapperCol) {
            this.booleanWrapperCol = booleanWrapperCol;
        }

        public BigDecimal getBigDecimalCol() {
            return bigDecimalCol;
        }

        public void setBigDecimalCol(BigDecimal bigDecimalCol) {
            this.bigDecimalCol = bigDecimalCol;
        }

        public BigInteger getBigIntegerCol() {
            return bigIntegerCol;
        }

        public void setBigIntegerCol(BigInteger bigIntegerCol) {
            this.bigIntegerCol = bigIntegerCol;
        }

        public String getStringCol() {
            return stringCol;
        }

        public void setStringCol(String stringCol) {
            this.stringCol = stringCol;
        }

        public InetAddress getInetCol() {
            return inetCol;
        }

        public void setInetCol(InetAddress inetCol) {
            this.inetCol = inetCol;
        }

        public Date getDateCol() {
            return dateCol;
        }

        public void setDateCol(Date dateCol) {
            this.dateCol = dateCol;
        }

        public UUID getUuidCol() {
            return uuidCol;
        }

        public void setUuidCol(UUID uuidCol) {
            this.uuidCol = uuidCol;
        }

        public UUID getTimeUuidCol() {
            return timeUuidCol;
        }

        public void setTimeUuidCol(UUID timeUuidCol) {
            this.timeUuidCol = timeUuidCol;
        }
    }
}
