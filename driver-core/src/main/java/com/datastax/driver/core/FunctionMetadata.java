/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes a CQL function (created with {@code CREATE FUNCTION...}).
 */
public class FunctionMetadata {
    private static final Logger logger = LoggerFactory.getLogger(FunctionMetadata.class);

    private final KeyspaceMetadata keyspace;
    private final String fullName;
    private final String simpleName;
    private final Map<String, DataType> arguments;
    private final String body;
    private final boolean calledOnNullInput;
    private final String language;
    private final DataType returnType;

    private FunctionMetadata(KeyspaceMetadata keyspace,
                             String fullName,
                             String simpleName,
                             Map<String, DataType> arguments,
                             String body,
                             boolean calledOnNullInput,
                             String language,
                             DataType returnType) {
        this.keyspace = keyspace;
        this.fullName = fullName;
        this.simpleName = simpleName;
        this.arguments = arguments;
        this.body = body;
        this.calledOnNullInput = calledOnNullInput;
        this.language = language;
        this.returnType = returnType;
    }

    // CREATE TABLE system.schema_functions (
    //     keyspace_name text,
    //     function_name text,
    //     signature frozen<list<text>>,
    //     argument_names list<text>,
    //     argument_types list<text>,
    //     body text,
    //     called_on_null_input boolean,
    //     language text,
    //     return_type text,
    //     PRIMARY KEY (keyspace_name, function_name, signature)
    // ) WITH CLUSTERING ORDER BY (function_name ASC, signature ASC)
    static FunctionMetadata build(KeyspaceMetadata ksm, Row row, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        String simpleName = row.getString("function_name");
        List<String> signature = row.getList("signature", String.class);
        String fullName = Metadata.fullFunctionName(simpleName, signature);

        List<String> argumentNames = row.getList("argument_names", String.class);
        List<String> argumentTypes = row.getList("argument_types", String.class);
        if (argumentNames.size() != argumentTypes.size()) {
            logger.error(String.format("Error parsing definition of function %1$s.%2$s: the number of argument names and types don't match."
                    + "Cluster.getMetadata().getKeyspace(\"%1$s\").getFunction(\"%2$s\") will be missing.",
                ksm.getName(), fullName));
            return null;
        }
        Map<String, DataType> arguments = buildArguments(argumentNames, argumentTypes, protocolVersion, codecRegistry);

        String body = row.getString("body");
        boolean calledOnNullInput = row.getBool("called_on_null_input");
        String language = row.getString("language");
        DataType returnType = CassandraTypeParser.parseOne(row.getString("return_type"), protocolVersion, codecRegistry);

        FunctionMetadata function = new FunctionMetadata(ksm, fullName, simpleName, arguments, body,
            calledOnNullInput, language, returnType);
        ksm.add(function);
        return function;
    }

    // Note: the caller ensures that names and types have the same size
    private static Map<String, DataType> buildArguments(List<String> names, List<String> types, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        if (names.isEmpty())
            return Collections.emptyMap();

        ImmutableMap.Builder<String, DataType> builder = ImmutableMap.builder();
        Iterator<String> iterTypes = types.iterator();
        for (String name : names) {
            DataType type = CassandraTypeParser.parseOne(iterTypes.next(), protocolVersion, codecRegistry);
            builder.put(name, type);
        }
        return builder.build();
    }

    /**
     * Returns a CQL query representing this function in human readable form.
     * <p>
     * This method is equivalent to {@link #asCQLQuery} but the output is formatted.
     *
     * @return the CQL query representing this function.
     */
    public String exportAsString() {
        return asCQLQuery(true);
    }

    /**
     * Returns a CQL query representing this function.
     *
     * This method returns a single 'CREATE FUNCTION' query corresponding to
     * this function definition.
     *
     * @return the 'CREATE FUNCTION' query corresponding to this function.
     */
    public String asCQLQuery() {
        return asCQLQuery(false);
    }

    @Override
    public String toString() {
        return asCQLQuery(false);
    }

    private String asCQLQuery(boolean formatted) {
        // create function test.sum(a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java as 'return a+b;';

        StringBuilder sb = new StringBuilder();

        sb
            .append("CREATE FUNCTION ")
            .append(Metadata.escapeId(keyspace.getName()))
            .append('.')
            .append(Metadata.escapeId(simpleName))
            .append('(');

        boolean first = true;
        for (Map.Entry<String, DataType> entry : arguments.entrySet()) {
            if (first)
                first = false;
            else
                sb.append(',');
            TableMetadata.newLine(sb, formatted);
            String name = entry.getKey();
            DataType type = entry.getValue();
            sb
                .append(TableMetadata.spaces(4, formatted))
                .append(Metadata.escapeId(name))
                .append(' ')
                .append(type);
        }
        sb.append(')');

        TableMetadata.spaceOrNewLine(sb, formatted)
            .append(calledOnNullInput ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT");

        TableMetadata.spaceOrNewLine(sb, formatted)
            .append("RETURNS ")
            .append(returnType);

        TableMetadata.spaceOrNewLine(sb, formatted)
            .append("LANGUAGE ")
            .append(language);

        TableMetadata.spaceOrNewLine(sb, formatted)
            .append("AS '")
            .append(body)
            .append("';");

        return sb.toString();
    }

    /**
     * Returns the keyspace this function belongs to.
     *
     * @return the keyspace metadata of the keyspace this function belongs to.
     */
    public KeyspaceMetadata getKeyspace() {
        return keyspace;
    }

    /**
     * Returns the full name of this function.
     * <p>
     * This is the name of the function, followed by the names of the argument types between parentheses,
     * for example {@code sum(int,int)}.
     *
     * @return the full name.
     */
    public String getFullName() {
        return fullName;
    }

    /**
     * Returns the simple name of this function.
     * <p>
     * This is the name of the function, without arguments. Note that functions can be overloaded with
     * different argument lists, therefore the simple name may not be unique. For example,
     * {@code sum(int,int)} and {@code sum(int,int,int)} both have the simple name {@code sum}.
     *
     * @return the simple name.
     *
     * @see #getFullName()
     */
    public String getSimpleName() {
        return simpleName;
    }

    /**
     * Returns the names and types of this function's arguments.
     *
     * @return a map from argument name to argument type.
     */
    public Map<String, DataType> getArguments() {
        return arguments;
    }

    /**
     * Returns the body of this function.
     *
     * @return the body.
     */
    public String getBody() {
        return body;
    }

    /**
     * Indicates whether this function's body gets called on null input.
     * <p>
     * This is {@code true} if the function was created with {@code CALLED ON NULL INPUT},
     * and {@code false} if it was created with {@code RETURNS NULL ON NULL INPUT}.
     *
     * @return whether this function's body gets called on null input.
     */
    public boolean isCalledOnNullInput() {
        return calledOnNullInput;
    }

    /**
     * Returns the programming language in which this function's body is written.
     *
     * @return the language.
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Returns the return type of this function.
     *
     * @return the return type.
     */
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof FunctionMetadata) {
            FunctionMetadata that = (FunctionMetadata)other;
            return this.keyspace.getName().equals(that.keyspace.getName()) &&
                this.fullName.equals(that.fullName) &&
                this.arguments.equals(that.arguments) &&
                this.body.equals(that.body) &&
                this.calledOnNullInput == that.calledOnNullInput &&
                this.language.equals(that.language) &&
                this.returnType.equals(that.returnType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keyspace.getName(), fullName, arguments, body, calledOnNullInput, language, returnType);
    }
}
