package com.datastax.driver.mapping;

import java.util.Map;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

import com.datastax.driver.mapping.EntityDefinition.ColumnDefinition;

public class ReflectionMapperTest {

    @Test
    public void testInheritance() {
        EntityDefinition<Product> entityDef = AnnotationParser.parseEntity(Product.class);
        assertNotNull(entityDef);

        ReflectionMapper<Product> mapper = new ReflectionMapper<Product>(entityDef);
        TV tv = new TV();
        tv.setModel("a");
        tv.setPrice(1.0f);
        tv.setProductId("b");
        tv.setVendor("c");
        tv.setScreenSize(52.0f);
        Map<String, Object> columns = mapper.entityToColumns(tv);

        assertEquals(columns.size(), 6);
        assertEquals(columns.get("model"), "a");
        assertEquals(columns.get("price"), 1.0f);
        assertEquals(columns.get("product_id"), "b");
        assertEquals(columns.get("vendor"), "c");
        assertEquals(columns.get("screen_size"), 52.0f);
        assertEquals(columns.get("product_type"), "tv");
    }
}
