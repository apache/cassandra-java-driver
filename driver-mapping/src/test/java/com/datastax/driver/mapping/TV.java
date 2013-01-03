package com.datastax.driver.mapping;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.InheritanceValue;

@InheritanceValue("tv")
public class TV extends Product {

	@Column(name = "screen_size")
	private float screenSize;

	public float getScreenSize() {
		return screenSize;
	}

	public void setScreenSize(float screenSize) {
		this.screenSize = screenSize;
	}

}
