package com.datastax.driver.core.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * <p>Annotation for a Class or Method that defines a Cassandra Version requirement.  If the cassandra verison in used
 * does not meet the version requirement, the test is skipped.</p>
 *
 * @see {@link com.datastax.driver.core.TestListener#beforeInvocation(org.testng.IInvokedMethod, org.testng.ITestResult)} for usage.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CassandraVersion {
    /**
     * @return The major version required to execute this test, i.e. "2.0"
     */
    public double major();

    /**
     * @return The minor version required to execute this test, i.e. "0"
     */
    public int minor() default 0;

    /**
     * @return The description returned if this version requirement is not met.
     */
    public String description() default "Does not meet minimum version requirement.";
}
