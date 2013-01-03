package com.datastax.driver.mapping;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.annotations.ClusteringKey;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(
	name = "message_by_recipient",
	defaultReadConsistencyLevel = ConsistencyLevel.QUORUM,
	defaultWriteConsistencyLevel = ConsistencyLevel.QUORUM
)
public class Message {
	
	@PartitionKey
	@Column(name = "from_user_id")
	private UUID fromUserId;
	
	@ClusteringKey(1)
	@Column(name = "to_user_id")
	private UUID toUserId;
	
	@ClusteringKey(2)
	@Column(name = "message_id")
	private UUID messageId;
	
	private Date date;
	
	private String title;
	
	private String body;
	
	private float f1;
	
	private double f2;
	
	private int f3;
	
	private long f4;
	
	private boolean f5;
	
	private short f6;
	
	private char f7;
	
	private byte f8;
		
	
	public char getF7() {
		return f7;
	}

	public void setF7(char f7) {
		this.f7 = f7;
	}

	public byte getF8() {
		return f8;
	}

	public void setF8(byte f8) {
		this.f8 = f8;
	}

	public float getF1() {
		return f1;
	}

	public void setF1(float f1) {
		this.f1 = f1;
	}

	public double getF2() {
		return f2;
	}

	public void setF2(double f2) {
		this.f2 = f2;
	}

	public int getF3() {
		return f3;
	}

	public void setF3(int f3) {
		this.f3 = f3;
	}

	public long getF4() {
		return f4;
	}

	public void setF4(long f4) {
		this.f4 = f4;
	}

	public boolean isF5() {
		return f5;
	}

	public void setF5(boolean f5) {
		this.f5 = f5;
	}

	public short getF6() {
		return f6;
	}

	public void setF6(short f6) {
		this.f6 = f6;
	}

	public UUID getFromUserId() {
		return fromUserId;
	}

	public void setFromUserId(UUID fromUserId) {
		this.fromUserId = fromUserId;
	}

	public UUID getToUserId() {
		return toUserId;
	}

	public void setToUserId(UUID toUserId) {
		this.toUserId = toUserId;
	}

	public UUID getMessageId() {
		return messageId;
	}

	public void setMessageId(UUID messageId) {
		this.messageId = messageId;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}
}
