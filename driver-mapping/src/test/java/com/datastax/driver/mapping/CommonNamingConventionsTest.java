package com.datastax.driver.mapping;

import com.datastax.driver.mapping.configuration.naming.CommonNamingConventions;
import com.datastax.driver.mapping.configuration.naming.NamingConvention;
import com.datastax.driver.mapping.configuration.naming.NamingStrategy;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for JAVA-1316 - test combinations of different
 * {@link com.datastax.driver.mapping.configuration.naming.CommonNamingConventions} implementation.
 */
public class CommonNamingConventionsTest {

    // test lower camel case inputs

    @Test(groups = "short")
    public void lower_camel_case_to_upper_camel_case() {
        test(
                new CommonNamingConventions.LowerCamelCase(),
                new CommonNamingConventions.UpperCamelCase(),
                "myXmlParser",
                "MyXmlParser"
        );
    }

    @Test(groups = "short")
    public void lower_camel_case_to_lower_snake_case() {
        test(
                new CommonNamingConventions.LowerCamelCase(),
                new CommonNamingConventions.LowerSnakeCase(),
                "myXmlParser",
                "my_xml_parser"
        );
    }

    @Test(groups = "short")
    public void lower_camel_case_to_upper_snake_case() {
        test(
                new CommonNamingConventions.LowerCamelCase(),
                new CommonNamingConventions.UpperSnakeCase(),
                "myXmlParser",
                "MY_XML_PARSER"
        );
    }

    @Test(groups = "short")
    public void lower_camel_case_to_lower_lisp_case() {
        test(
                new CommonNamingConventions.LowerCamelCase(),
                new CommonNamingConventions.LowerLispCase(),
                "myXmlParser",
                "my-xml-parser"
        );
    }

    @Test(groups = "short")
    public void lower_camel_case_to_upper_lisp_case() {
        test(
                new CommonNamingConventions.LowerCamelCase(),
                new CommonNamingConventions.UpperLispCase(),
                "myXmlParser",
                "MY-XML-PARSER"
        );
    }

    @Test(groups = "short")
    public void lower_camel_case_to_lower_case() {
        test(
                new CommonNamingConventions.LowerCamelCase(),
                new CommonNamingConventions.LowerCase(),
                "myXmlParser",
                "myxmlparser"
        );
    }

    // test upper camel case inputs

    @Test(groups = "short")
    public void upper_camel_case_to_lower_camel_case() {
        test(
                new CommonNamingConventions.UpperCamelCase(),
                new CommonNamingConventions.LowerCamelCase(),
                "MyXmlParser",
                "myXmlParser"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_to_lower_snake_case() {
        test(
                new CommonNamingConventions.UpperCamelCase(),
                new CommonNamingConventions.LowerSnakeCase(),
                "MyXmlParser",
                "my_xml_parser"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_to_upper_snake_case() {
        test(
                new CommonNamingConventions.UpperCamelCase(),
                new CommonNamingConventions.UpperSnakeCase(),
                "MyXmlParser",
                "MY_XML_PARSER"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_to_lower_lisp_case() {
        test(
                new CommonNamingConventions.UpperCamelCase(),
                new CommonNamingConventions.LowerLispCase(),
                "MyXmlParser",
                "my-xml-parser"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_to_upper_lisp_case() {
        test(
                new CommonNamingConventions.UpperCamelCase(),
                new CommonNamingConventions.UpperLispCase(),
                "MyXmlParser",
                "MY-XML-PARSER"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_to_lower_case() {
        test(
                new CommonNamingConventions.UpperCamelCase(),
                new CommonNamingConventions.LowerCase(),
                "MyXmlParser",
                "myxmlparser"
        );
    }

    // test lower snake case inputs

    @Test(groups = "short")
    public void lower_snake_case_to_lower_camel_case() {
        test(
                new CommonNamingConventions.LowerSnakeCase(),
                new CommonNamingConventions.LowerCamelCase(),
                "my_xml_parser",
                "myXmlParser"
        );
    }

    @Test(groups = "short")
    public void lower_snake_case_to_upper_camel_case() {
        test(
                new CommonNamingConventions.LowerSnakeCase(),
                new CommonNamingConventions.UpperCamelCase(),
                "my_xml_parser",
                "MyXmlParser"
        );
    }

    @Test(groups = "short")
    public void lower_snake_case_to_upper_snake_case() {
        test(
                new CommonNamingConventions.LowerSnakeCase(),
                new CommonNamingConventions.UpperSnakeCase(),
                "my_xml_parser",
                "MY_XML_PARSER"
        );
    }

    @Test(groups = "short")
    public void lower_snake_case_to_lower_lisp_case() {
        test(
                new CommonNamingConventions.LowerSnakeCase(),
                new CommonNamingConventions.LowerLispCase(),
                "my_xml_parser",
                "my-xml-parser"
        );
    }

    @Test(groups = "short")
    public void lower_snake_case_to_upper_lisp_case() {
        test(
                new CommonNamingConventions.LowerSnakeCase(),
                new CommonNamingConventions.UpperLispCase(),
                "my_xml_parser",
                "MY-XML-PARSER"
        );
    }

    @Test(groups = "short")
    public void lower_snake_case_to_lower_case() {
        test(
                new CommonNamingConventions.LowerSnakeCase(),
                new CommonNamingConventions.LowerCase(),
                "my_xml_parser",
                "myxmlparser"
        );
    }

    // test upper snake case inputs

    @Test(groups = "short")
    public void upper_snake_case_to_lower_camel_case() {
        test(
                new CommonNamingConventions.UpperSnakeCase(),
                new CommonNamingConventions.LowerCamelCase(),
                "MY_XML_PARSER",
                "myXmlParser"
        );
    }

    @Test(groups = "short")
    public void upper_snake_case_to_upper_camel_case() {
        test(
                new CommonNamingConventions.UpperSnakeCase(),
                new CommonNamingConventions.UpperCamelCase(),
                "MY_XML_PARSER",
                "MyXmlParser"
        );
    }

    @Test(groups = "short")
    public void upper_snake_case_to_lower_snake_case() {
        test(
                new CommonNamingConventions.UpperSnakeCase(),
                new CommonNamingConventions.LowerSnakeCase(),
                "MY_XML_PARSER",
                "my_xml_parser"
        );
    }

    @Test(groups = "short")
    public void upper_snake_case_to_lower_lisp_case() {
        test(
                new CommonNamingConventions.UpperSnakeCase(),
                new CommonNamingConventions.LowerLispCase(),
                "MY_XML_PARSER",
                "my-xml-parser"
        );
    }

    @Test(groups = "short")
    public void upper_snake_case_to_upper_lisp_case() {
        test(
                new CommonNamingConventions.UpperSnakeCase(),
                new CommonNamingConventions.UpperLispCase(),
                "MY_XML_PARSER",
                "MY-XML-PARSER"
        );
    }

    @Test(groups = "short")
    public void upper_snake_case_to_lower_case() {
        test(
                new CommonNamingConventions.UpperSnakeCase(),
                new CommonNamingConventions.LowerCase(),
                "MY_XML_PARSER",
                "myxmlparser"
        );
    }

    // test lower lisp case inputs

    @Test(groups = "short")
    public void lower_lisp_case_to_lower_camel_case() {
        test(
                new CommonNamingConventions.LowerLispCase(),
                new CommonNamingConventions.LowerCamelCase(),
                "my-xml-parser",
                "myXmlParser"
        );
    }

    @Test(groups = "short")
    public void lower_lisp_case_to_upper_camel_case() {
        test(
                new CommonNamingConventions.LowerLispCase(),
                new CommonNamingConventions.UpperCamelCase(),
                "my-xml-parser",
                "MyXmlParser"
        );
    }

    @Test(groups = "short")
    public void lower_lisp_case_to_lower_snake_case() {
        test(
                new CommonNamingConventions.LowerLispCase(),
                new CommonNamingConventions.LowerSnakeCase(),
                "my-xml-parser",
                "my_xml_parser"
        );
    }

    @Test(groups = "short")
    public void lower_lisp_case_to_upper_snake_case() {
        test(
                new CommonNamingConventions.LowerLispCase(),
                new CommonNamingConventions.UpperSnakeCase(),
                "my-xml-parser",
                "MY_XML_PARSER"
        );
    }

    @Test(groups = "short")
    public void lower_lisp_case_to_upper_lisp_case() {
        test(
                new CommonNamingConventions.LowerLispCase(),
                new CommonNamingConventions.UpperLispCase(),
                "my-xml-parser",
                "MY-XML-PARSER"
        );
    }

    @Test(groups = "short")
    public void lower_lisp_case_to_lower_case() {
        test(
                new CommonNamingConventions.LowerLispCase(),
                new CommonNamingConventions.LowerCase(),
                "my-xml-parser",
                "myxmlparser"
        );
    }

    // test upper lisp case inputs

    @Test(groups = "short")
    public void upper_lisp_case_to_lower_camel_case() {
        test(
                new CommonNamingConventions.UpperLispCase(),
                new CommonNamingConventions.LowerCamelCase(),
                "MY-XML-PARSER",
                "myXmlParser"
        );
    }

    @Test(groups = "short")
    public void upper_lisp_case_to_upper_camel_case() {
        test(
                new CommonNamingConventions.UpperLispCase(),
                new CommonNamingConventions.UpperCamelCase(),
                "MY-XML-PARSER",
                "MyXmlParser"
        );
    }

    @Test(groups = "short")
    public void upper_lisp_case_to_lower_snake_case() {
        test(
                new CommonNamingConventions.UpperLispCase(),
                new CommonNamingConventions.LowerSnakeCase(),
                "MY-XML-PARSER",
                "my_xml_parser"
        );
    }

    @Test(groups = "short")
    public void upper_lisp_case_to_upper_snake_case() {
        test(
                new CommonNamingConventions.UpperLispCase(),
                new CommonNamingConventions.UpperSnakeCase(),
                "MY-XML-PARSER",
                "MY_XML_PARSER"
        );
    }

    @Test(groups = "short")
    public void upper_lisp_case_to_lower_lisp_case() {
        test(
                new CommonNamingConventions.UpperLispCase(),
                new CommonNamingConventions.LowerLispCase(),
                "MY-XML-PARSER",
                "my-xml-parser"
        );
    }

    @Test(groups = "short")
    public void upper_lisp_case_to_lower_case() {
        test(
                new CommonNamingConventions.UpperLispCase(),
                new CommonNamingConventions.LowerCase(),
                "MY-XML-PARSER",
                "myxmlparser"
        );
    }

    // test special camel case settings

    @Test(groups = "short")
    public void lower_camel_case_with_prefix_to_upper_camel_case() {
        test(
                new CommonNamingConventions.LowerCamelCase("_"),
                new CommonNamingConventions.UpperCamelCase(),
                "_myXmlParser",
                "MyXmlParser"
        );
        test(
                new CommonNamingConventions.LowerCamelCase("_"),
                new CommonNamingConventions.UpperCamelCase(),
                "myXmlParser",
                "MyXmlParser"
        );
        test(
                new CommonNamingConventions.LowerCamelCase("_", "m"),
                new CommonNamingConventions.UpperCamelCase(),
                "mMyXmlParser",
                "MyXmlParser"
        );
        test(
                new CommonNamingConventions.LowerCamelCase("_", "m"),
                new CommonNamingConventions.UpperCamelCase(),
                "_MyXmlParser",
                "MyXmlParser"
        );
    }

    @Test(groups = "short")
    public void lower_camel_case_with_prefix_to_lower_snake_case() {
        test(
                new CommonNamingConventions.LowerCamelCase("_"),
                new CommonNamingConventions.LowerSnakeCase(),
                "_myXmlParser",
                "my_xml_parser"
        );
        test(
                new CommonNamingConventions.LowerCamelCase("_"),
                new CommonNamingConventions.LowerSnakeCase(),
                "myXmlParser",
                "my_xml_parser"
        );
        test(
                new CommonNamingConventions.LowerCamelCase("_", "m"),
                new CommonNamingConventions.LowerSnakeCase(),
                "mMyXmlParser",
                "my_xml_parser"
        );
        test(
                new CommonNamingConventions.LowerCamelCase("_", "m"),
                new CommonNamingConventions.LowerSnakeCase(),
                "_MyXmlParser",
                "my_xml_parser"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_with_prefix_to_upper_camel_case() {
        test(
                new CommonNamingConventions.UpperCamelCase("_"),
                new CommonNamingConventions.UpperCamelCase(),
                "_MyXmlParser",
                "MyXmlParser"
        );
        test(
                new CommonNamingConventions.UpperCamelCase("_"),
                new CommonNamingConventions.UpperCamelCase(),
                "MyXmlParser",
                "MyXmlParser"
        );
        test(
                new CommonNamingConventions.UpperCamelCase("_", "m"),
                new CommonNamingConventions.UpperCamelCase(),
                "mMyXmlParser",
                "MyXmlParser"
        );
        test(
                new CommonNamingConventions.UpperCamelCase("_", "m"),
                new CommonNamingConventions.UpperCamelCase(),
                "_MyXmlParser",
                "MyXmlParser"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_with_prefix_to_upper_snake_case() {
        test(
                new CommonNamingConventions.UpperCamelCase("_"),
                new CommonNamingConventions.LowerSnakeCase(),
                "_MyXmlParser",
                "my_xml_parser"
        );
        test(
                new CommonNamingConventions.UpperCamelCase("_"),
                new CommonNamingConventions.LowerSnakeCase(),
                "MyXmlParser",
                "my_xml_parser"
        );
        test(
                new CommonNamingConventions.UpperCamelCase("_", "m"),
                new CommonNamingConventions.LowerSnakeCase(),
                "mMyXmlParser",
                "my_xml_parser"
        );
        test(
                new CommonNamingConventions.UpperCamelCase("_", "m"),
                new CommonNamingConventions.LowerSnakeCase(),
                "_MyXmlParser",
                "my_xml_parser"
        );
    }

    @Test(groups = "short")
    public void lower_camel_case_with_abbr_to_lower_camel_case_with_no_abbr() {
        test(
                new CommonNamingConventions.LowerCamelCase(true),
                new CommonNamingConventions.LowerCamelCase(false),
                "myXMLParser",
                "myXmlParser"
        );
    }

    @Test(groups = "short")
    public void upper_camel_case_with_abbr_to_lower_camel_case_with_abbr() {
        test(
                new CommonNamingConventions.UpperCamelCase(true),
                new CommonNamingConventions.LowerCamelCase(true),
                "MyXMLParser",
                "myXMLParser"
        );
    }

    private void test(NamingConvention inputConvention, NamingConvention outputConvention, String input, String output) {
        NamingStrategy namingStrategy = new NamingStrategy(inputConvention, outputConvention);
        String result = namingStrategy.toCassandra(input);
        assertThat(result).isEqualTo(output);
    }

}
