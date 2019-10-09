/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.insights;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class PackageUtilTest {

  private static final String DEFAULT_PACKAGE = "default.package";

  @Test
  public void should_find_package_name_for_class() {
    // given
    TestClass testClass = new TestClass();

    // then
    String namespace = PackageUtil.getNamespace(testClass.getClass());

    // then
    assertThat(namespace).isEqualTo("com.datastax.dse.driver.internal.core.insights");
  }

  @Test
  @UseDataProvider("packagesProvider")
  public void should_get_full_package_or_return_default(String fullClassSetting, String expected) {
    // when
    String result = PackageUtil.getFullPackageOrDefault(fullClassSetting, DEFAULT_PACKAGE);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  @UseDataProvider("classesProvider")
  public void should_get_class_name_from_full_class_setting(
      String fullClassSetting, String expected) {
    // when
    String result = PackageUtil.getClassName(fullClassSetting);

    // then
    assertThat(result).isEqualTo(expected);
  }

  @DataProvider
  public static Object[][] packagesProvider() {
    return new Object[][] {
      {"com.P", "com"},
      {"ClassName", DEFAULT_PACKAGE},
      {"", DEFAULT_PACKAGE},
      {"com.p.a.2.x.12.Class", "com.p.a.2.x.12"},
    };
  }

  @DataProvider
  public static Object[][] classesProvider() {
    return new Object[][] {
      {"com.P", "P"},
      {"ClassName", "ClassName"},
      {"", ""},
      {"com.p.a.2.x.12.Class", "Class"},
    };
  }

  private static class TestClass {}
}
