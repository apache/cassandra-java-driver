/*
 * Copyright 2014 LEVEL5.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.driver.core;

import com.datastax.driver.core.CCMBridge.PerClassSingleNodeCluster;
import com.datastax.driver.core.utils.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Tests the use of User Defined Types.
 * 
 * @author flarcher
 */
public class UserDefinedTypeTest extends PerClassSingleNodeCluster {

	@Override
	protected Collection<String> getTableDefinitions() {
		final List<String> stmts = new ArrayList<String>();
		
		/** Address example */
		stmts.add("CREATE TYPE address ( street text, city text, zip int )");
		stmts.add("CREATE TABLE customer (login ascii, address address, PRIMARY KEY(login))");
		
		/** Many addresses example */
		stmts.add("CREATE TABLE profile (login ascii PRIMARY KEY, addresses map<text, address>)");
		
		/** Phone example */
		stmts.add("CREATE TYPE phone ( number text, tags set<text> )");
		stmts.add("CREATE TABLE user (login ascii PRIMARY KEY, tel phone)");
		
		/** recursive UDT */
		stmts.add("CREATE TYPE contact_info ( address address, phone text )");
		stmts.add("CREATE TABLE contact (login ascii PRIMARY KEY, info contact_info)");
		
		return stmts;
	}

	/** Address example */
	private static final String CUSTOMER_LOGIN = "georges";
	private static final String CUSTOMER_STREET = "rue de la Grange";
	private static final String CUSTOMER_CITY = "Trifoullies les oies";
	private static final int CUSTOMER_ZIP = 42123;
	
	/** Many addresses example */
	private static final String OFFICE_STREET = "grande rue";
	private static final String OFFICE_CITY = "Bourgade";
	private static final int OFFICE_ZIP = 12056;
	
	/* Phone example */
	private static final String PHONE_NUMBER = "123456789";
	private static final String PHONE_TAG1 = "preferred";
	private static final String PHONE_TAG2 = "direct line";
	
	@Test(groups = "unit")
	public void testUdtCustomTypeClassNameParsing() {
		
		// This class name represents a UDT that contains another UDT (real life example)
		final String udtInUdtClassName = "org.apache.cassandra.db.marshal.UserType(test,636f6e746163745f696e666f,"
				+ "61646472657373:org.apache.cassandra.db.marshal.UserType(test,61646472657373,"
				+   "737472656574:org.apache.cassandra.db.marshal.UTF8Type,"
				+   "63697479:org.apache.cassandra.db.marshal.UTF8Type,"
				+   "7a6970:org.apache.cassandra.db.marshal.Int32Type),"
				+ "70686f6e655f6e756d626572:org.apache.cassandra.db.marshal.UTF8Type)";
		
		final CassandraTypeParser.UserDefinedTypeDefinition def = CassandraTypeParser.parseUserDefinedType(udtInUdtClassName);
		assertNotNull(def);
		assertEquals("test",def.keySpace);
		assertEquals("contact_info", def.name);
		assertEquals(2, def.columns.size());
		
		final DataType phoneNumberDataType = def.columns.get("phone_number");
		assertNotNull(phoneNumberDataType);
		assertEquals(DataType.Name.TEXT, phoneNumberDataType.getName());
		
		final DataType addressDataType = def.columns.get("address");
		assertNotNull(addressDataType);
		assertEquals(DataType.Name.CUSTOM, addressDataType.getName());
		assertEquals("org.apache.cassandra.db.marshal.UserType(test,61646472657373,"
				+ "737472656574:org.apache.cassandra.db.marshal.UTF8Type,"
				+ "63697479:org.apache.cassandra.db.marshal.UTF8Type,"
				+ "7a6970:org.apache.cassandra.db.marshal.Int32Type)", addressDataType.getCustomTypeClassName());
		
		final CassandraTypeParser.UserDefinedTypeDefinition subDef = CassandraTypeParser.parseUserDefinedType(addressDataType.getCustomTypeClassName());
		assertNotNull(subDef);
		assertEquals("test",subDef.keySpace);
		assertEquals("address", subDef.name);
		assertEquals(3, subDef.columns.size());
		
		final DataType streetDataType = subDef.columns.get("street");
		assertNotNull(streetDataType);
		assertEquals(DataType.Name.TEXT, streetDataType.getName());
		
		final DataType zipDataType = subDef.columns.get("zip");
		assertNotNull(zipDataType);
		assertEquals(DataType.Name.INT, zipDataType.getName());
	}
	
	
	@Test(groups = "short")
	public void simpleUdtTest() {
		
		session.execute("INSERT INTO customer (login, address) VALUES ('" + CUSTOMER_LOGIN + "', "
				+ "{street: '" + CUSTOMER_STREET + "', city: '" + CUSTOMER_CITY + "', zip: " + CUSTOMER_ZIP + "});");

		final ResultSet rs = session.execute("SELECT address FROM customer WHERE login = '" + CUSTOMER_LOGIN + "';");
		assertNotNull(rs);
		final Row row = rs.one();
		assertNotNull(row);
		final Row udtValue = row.getUDT("address");
		assertNotNull(udtValue);
		assertEquals(3, udtValue.getColumnDefinitions().size());
		assertEquals(CUSTOMER_STREET, udtValue.getString("street"));
		assertEquals(CUSTOMER_CITY, udtValue.getString("city"));
		assertEquals(CUSTOMER_ZIP, udtValue.getInt("zip"));

		session.execute("DELETE FROM customer WHERE login = '" + CUSTOMER_LOGIN + "';");
	}

	private void testPhoneTags(Set<String> tags) {
		assertNotNull(tags);
		assertEquals(2, tags.size());
		assertTrue(tags.contains(PHONE_TAG1));
		assertTrue(tags.contains(PHONE_TAG2));
	}

	//@Test(groups="short") /* Do not work for now */
	public void collectionInUdtTest() {
		
		session.execute("INSERT INTO user (login, tel) VALUES ('" + CUSTOMER_LOGIN + "', "
				+ "{number: '" + PHONE_NUMBER + "', tags: { '" + PHONE_TAG1 + "', '" + PHONE_TAG2 + "'}});");

		final ResultSet rs = session.execute("SELECT tel FROM user WHERE login = '" + CUSTOMER_LOGIN + "';");
		assertNotNull(rs);
		final Row row = rs.one();
		assertNotNull(row);
		final Row udtValue = row.getUDT("tel");
		assertNotNull(udtValue);
		assertEquals(PHONE_NUMBER, udtValue.getString("number"));
		
		final DataType udtDt = row.getColumnDefinitions().getType("tel");
		assertEquals(udtDt.getName(), DataType.Name.CUSTOM);
		final CassandraTypeParser.UserDefinedTypeDefinition def = CassandraTypeParser.parseUserDefinedType(udtDt.getCustomTypeClassName());
		final DataType udtTagsDt = def.columns.get("tags");
		assertEquals(DataType.Name.SET, udtTagsDt.getName());
		assertNotNull(udtTagsDt.getTypeArguments());
		assertEquals(1, udtTagsDt.getTypeArguments().size());
		assertEquals(udtTagsDt.getTypeArguments().get(0).getName(), DataType.Name.TEXT);
		
		final DataType tagsDt = udtValue.getColumnDefinitions().getType("tags");
		assertEquals(DataType.Name.SET, tagsDt.getName());
		assertNotNull(tagsDt.getTypeArguments());
		assertEquals(1, tagsDt.getTypeArguments().size());
		assertEquals(tagsDt.getTypeArguments().get(0).getName(), DataType.Name.TEXT);
		
		final TypeCodec<List<ByteBuffer>> udtCodec = new TypeCodec.UserDefinedTypeCodec(def.columns.size());
		final List<ByteBuffer> buffers = udtCodec.deserialize(row.getBytesUnsafe("tel"));
		assertNotNull(buffers.get(1));
		System.out.println("### value =" + Bytes.toHexString(buffers.get(1)));
		final TypeCodec<String> strCodec = TypeCodec.createFor(DataType.Name.TEXT);
		final TypeCodec.SetCodec<String> setCodec = new TypeCodec.SetCodec<String>(strCodec);
		testPhoneTags(setCodec.deserialize(buffers.get(1)));
		testPhoneTags(udtValue.getSet("tags", String.class));
		
		session.execute("DELETE FROM user WHERE login = '" + CUSTOMER_LOGIN + "';");
	}

	@Test(groups="short")
	public void recursiveUdtTest() {
		
		session.execute("INSERT INTO contact (login, info) VALUES ('" + CUSTOMER_LOGIN + "', {"
				+ " address : {street: '" + CUSTOMER_STREET + "', city: '" + CUSTOMER_CITY + "', zip: " + CUSTOMER_ZIP + "}, "
				+ " phone : '" + PHONE_NUMBER + "' });");

		final ResultSet rs = session.execute("SELECT info FROM contact WHERE login = '" + CUSTOMER_LOGIN + "';");
		assertNotNull(rs);
		final Row row = rs.one();
		assertNotNull(row);
		final Row address = row.getUDT("address");
		assertNotNull(address);
		assertEquals(CUSTOMER_STREET, address.getString("street"));
		assertEquals(CUSTOMER_CITY, address.getString("city"));
		assertEquals(CUSTOMER_ZIP, address.getInt("zip"));
		
		final String phone = row.getString("phone");
		assertNotNull(phone);
		assertEquals(PHONE_NUMBER, phone);
		
		session.execute("DELETE FROM contact WHERE login = '" + CUSTOMER_LOGIN + "';");
	}
	

	//@Test(groups="short") /* Do not work for now */
	public void udtInCollectionTest() {
		
		session.execute("INSERT INTO profile (login, addresses) VALUES ('" + CUSTOMER_LOGIN + "', {"
				+ "'home' : {street: '" + CUSTOMER_STREET + "', city: '" + CUSTOMER_CITY + "', zip: " + CUSTOMER_ZIP + "},"
				+ "'office' : {street: '" + OFFICE_STREET + "', city: '" + OFFICE_CITY + "', zip: " + OFFICE_ZIP + "}});");

		final ResultSet rs = session.execute("SELECT addresses FROM profile WHERE login = '" + CUSTOMER_LOGIN + "';");
		assertNotNull(rs);
		final Row row = rs.one();
		assertNotNull(row);
		
		// TODO

		session.execute("DELETE FROM profile WHERE login = '" + CUSTOMER_LOGIN + "';");
	}
	
}
