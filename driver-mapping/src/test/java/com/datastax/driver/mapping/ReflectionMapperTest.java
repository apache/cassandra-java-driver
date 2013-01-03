package com.datastax.driver.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.Test;

import com.datastax.driver.mapping.EntityDefinition.ColumnDefinition;

public class ReflectionMapperTest {

	@Test
	public void testInheritance() {
		EntityDefinition entityDef = AnnotationParser.parseEntity(Product.class);
		assertNotNull(entityDef);
		
		ReflectionMapper mapper = new ReflectionMapper(entityDef);
		TV tv = new TV();
		tv.setModel("a");
		tv.setPrice(1.0f);
		tv.setProductId("b");
		tv.setVendor("c");
		tv.setScreenSize(52.0f);
		Map<String, Object> columns = mapper.entityToColumns(tv);
		
		assertEquals(6, columns.size());
		assertEquals("a", columns.get("model"));
		assertEquals(1.0f, columns.get("price"));
		assertEquals("b", columns.get("product_id"));
		assertEquals("c", columns.get("vendor"));
		assertEquals(52.0f, columns.get("screen_size"));
		assertEquals("tv", columns.get("product_type"));
		
		
	}
	
	public void testRowToEntity() {
		EntityDefinition entityDef = AnnotationParser.parseEntity(Product.class);
		assertNotNull(entityDef);
		
		ReflectionMapper mapper = new ReflectionMapper(entityDef);
		
	}
}
