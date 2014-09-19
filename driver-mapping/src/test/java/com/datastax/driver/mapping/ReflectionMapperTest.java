package com.datastax.driver.mapping;

import com.datastax.driver.core.DataType;
import junit.framework.TestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ReflectionMapperTest extends TestCase {
    @Test
    public void bigdecimal_should_be_mapped_to_the_decimal_datatype() {
        Assert.assertEquals(ReflectionMapper.getSimpleType(BigDecimal.class, null), DataType.decimal());
    }
    @Test
    public void biginteger_should_be_mapped_to_the_varint_datatype() {
        Assert.assertEquals(ReflectionMapper.getSimpleType(BigInteger.class, null), DataType.varint());
    }
}