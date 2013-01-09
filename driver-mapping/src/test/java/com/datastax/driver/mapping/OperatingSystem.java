package com.datastax.driver.mapping;

import com.datastax.driver.mapping.annotations.EnumValue;

public enum OperatingSystem {

    @EnumValue("os_android")
    ANDROID,

    @EnumValue("os_ios")
    IOS;
}
