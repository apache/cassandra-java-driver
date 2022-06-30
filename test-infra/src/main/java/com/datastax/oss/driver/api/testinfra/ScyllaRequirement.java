package com.datastax.oss.driver.api.testinfra;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation for a Class or Method that defines a Scylla Version requirement. If the Scylla version
 * in use does not meet the version requirement, the test is skipped.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ScyllaRequirement {
  /** @return The minimum Enterprise version required to execute this test, i.e. "2020.1.13" */
  String minEnterprise() default "";

  /**
   * @return the maximum exclusive Enterprise version allowed to execute this test, i.e. "2021.1.12"
   *     means only tests &lt; "2021.1.12" may execute this test.
   */
  String maxEnterprise() default "";

  /** @return The minimum OSS version required to execute this test, i.e. "4.5.6" */
  String minOSS() default "";

  /**
   * @return the maximum exclusive OSS version allowed to execute this test, i.e. "5.0.0" means only
   *     tests &lt; "5.0.0" may execute this test.
   */
  String maxOSS() default "";

  /** @return The description returned if this version requirement is not met. */
  String description() default "Does not meet Scylla version requirement.";
}
