/*
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

import com.datastax.driver.core.exceptions.DriverInternalError;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Represents a User Defined Type Value (UDT).
 * <br/>
 * Is immutable.
 * 
 * @author flarcher
 */
public class UDTValue extends ArrayBackedRow implements Row {
	
	static UDTValue fromData(String keySpace, String parentName, ColumnDefinitions columnDefinitions, List<ByteBuffer> columnValues) {
		
		if (columnValues == null) {
			return null;
		}
		
		return new UDTValue(keySpace, parentName, columnDefinitions, columnValues);
	}
	
	private UDTValue(String keySpace, String parentName, ColumnDefinitions columnDefinitions, List<ByteBuffer> columnValues) {
		super(columnDefinitions, columnValues);
		if (keySpace == null || keySpace.isEmpty()) {
			throw new DriverInternalError("Empty keyspace");
		}
		this.keySpace = keySpace;
		
		if (parentName == null || parentName.isEmpty()) {
			throw new DriverInternalError("Empty parent");
		}
		this.parentName = parentName;
		
	}
	
	private final String keySpace;
	private final String parentName;
	
	/**
	 * @return  The key-space where the UDT is defined.
	 */
	public String getKeySpace() {
		return keySpace;
	}
	
	/**
	 * @return The name of the table or UDT that holds this UDT.
	 */
	public String getParent() {
		return parentName;
	}
	
	/**
	 * @return The name of the UDT.
	 */
	public String getName() {
		return getColumnDefinitions().getTable(0);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
        sb.append("UDT{").append(keySpace).append(",").append(parentName).append(",[");
        for (int i = 0; i < getColumnDefinitions().size(); i++) {
            if (i != 0)
                sb.append(", ");
            ByteBuffer bb = getBytesUnsafe(i);
            if (bb == null)
                sb.append("NULL");
            else
                sb.append(getColumnDefinitions().getType(i).codec().deserialize(bb).toString());
        }
		sb.append("]}");
        return sb.toString();
	}

	@Override
	protected TypeCodec<Object> getCodec(DataType type) {
		if (type.isCollection()) {
			// We retrieve the cached instance that supports CQL v3
			return ((TypeCodec.CollectionCodec<Object>) type.codec()).getVersion(false);
		} else {
			return type.codec();
		}
	}

}
