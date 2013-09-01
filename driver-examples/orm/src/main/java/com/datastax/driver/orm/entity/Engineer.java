package com.datastax.driver.orm.entity;

import com.datastax.driver.core.orm.mapping.Column;
import com.datastax.driver.core.orm.mapping.ColumnFamily;
import com.datastax.driver.core.orm.mapping.Key;

@ColumnFamily
public class Engineer extends Worker {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6643883283637783076L;

	@Key
	private String nickName;
	
	@Column
	private String type;
	
	@Column
	private String especialization;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getEspecialization() {
		return especialization;
	}

	public void setEspecialization(String especialization) {
		this.especialization = especialization;
	}

	public String getNickName() {
		return nickName;
	}

	public void setNickName(String nickName) {
		this.nickName = nickName;
	}
	
	
	
	
}
