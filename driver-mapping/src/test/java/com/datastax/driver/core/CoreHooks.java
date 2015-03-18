package com.datastax.driver.core;

import java.util.Collections;

/**
 * Provides access to package-private members of the core module.
 */
public class CoreHooks {
    public static UserType MOCK_USER_TYPE = new UserType("mockKs", "mockUDT", Collections.<UserType.Field>emptyList());
}
