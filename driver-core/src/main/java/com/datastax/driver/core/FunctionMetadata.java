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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Describes a CQL function (created with {@code CREATE FUNCTION...}).
 */
public class FunctionMetadata {
    private static final Logger logger = LoggerFactory.getLogger(FunctionMetadata.class);

    private final KeyspaceMetadata keyspace;
    private final String simpleName;
    private final Map<String, DataType> arguments;
    private final String body;
    private final boolean calledOnNullInput;
    private final String language;
    private final DataType returnType;

    private FunctionMetadata(KeyspaceMetadata keyspace,
                             String simpleName,
                             Map<String, DataType> arguments,
                             String body,
                             boolean calledOnNullInput,
                             String language,
                             DataType returnType) {
        this.keyspace = keyspace;
        this.simpleName = simpleName;
        this.arguments = arguments;
        this.body = body;
        this.calledOnNullInput = calledOnNullInput;
        this.language = language;
        this.returnType = returnType;
    }

    // Cassandra < 3.0:
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
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.functions (
    //     keyspace_name text,
    //     function_name text,
    //     argument_names frozen<list<text>>,
    //     argument_types frozen<list<text>>,
    //     body text,
    //     called_on_null_input boolean,
    //     language text,
    //     return_type text,
    //     PRIMARY KEY (keyspace_name, function_name, argument_types)
    // ) WITH CLUSTERING ORDER BY (function_name ASC, argument_types ASC)
    //

    static FunctionMetadata build(KeyspaceMetadata ksm, Row row, VersionNumber version, Cluster cluster) {
        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        String simpleName = row.getString("function_name");
        List<String> argumentNames = row.getList("argument_names", String.class);
        // this will be a list of C* types in 2.2 and a list of CQL types in 3.0
        List<String> argumentTypes = row.getList("argument_types", String.class);
        Map<String, DataType> arguments = buildArguments(ksm, argumentNames, argumentTypes, version, cluster);
        if (argumentNames.size() != argumentTypes.size()) {
            String fullName = Metadata.fullFunctionName(simpleName, arguments.values());
            logger.error(String.format("Error parsing definition of function %1$s.%2$s: the number of argument names and types don't match."
                            + "Cluster.getMetadata().getKeyspace(\"%1$s\").getFunction(\"%2$s\") will be missing.",
                    ksm.getName(), fullName));
            return null;
        }
        String body = row.getString("body");
        boolean calledOnNullInput = row.getBool("called_on_null_input");
        String language = row.getString("language");
        DataType returnType;
        if (version.getMajor() >= 3.0) {
            returnType = DataTypeCqlNameParser.parse(row.getString("return_type"), cluster, ksm.getName(), ksm.userTypes, null, false, false);
        } else {
            returnType = DataTypeClassNameParser.parseOne(row.getString("return_type"), protocolVersion, codecRegistry);
        }
        return new FunctionMetadata(ksm, simpleName, arguments, body, calledOnNullInput, language, returnType);
    }

    // Note: the caller ensures that names and types have the same size
    private static Map<String, DataType> buildArguments(KeyspaceMetadata ksm, List<String> names, List<String> types, VersionNumber version, Cluster cluster) {
        if (names.isEmpty())
            return Collections.emptyMap();
        ImmutableMap.Builder<String, DataType> builder = ImmutableMap.builder();
        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        Iterator<String> iterTypes = types.iterator();
        for (String name : names) {
            DataType type;
            if (version.getMajor() >= 3) {
                type = DataTypeCqlNameParser.parse(iterTypes.next(), cluster, ksm.getName(), ksm.userTypes, null, false, false);
            } else {
                type = DataTypeClassNameParser.parseOne(iterTypes.next(), protocolVersion, codecRegistry);
            }
            builder.put(name, type);
        }
        return builder.build();
    }

    /**
     * Returns a CQL query representing this function in human readable form.
     * <p/>
     * This method is equivalent to {@link #asCQLQuery} but the output is formatted.
     *
     * @return the CQL query representing this function.
     */
    public String exportAsString() {
        return asCQLQuery(true);
    }

    /**
     * Returns a CQL query representing this function.
     * <p/>
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

        StringBuilder sb = new StringBuilder("CREATE FUNCTION ");

        sb
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
                    .append(type.asFunctionParameterString());
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
     * Returns the CQL signature of this function.
     * <p/>
     * This is the name of the function, followed by the names of the argument types between parentheses,
     * for example {@code sum(int,int)}.
     * <p/>
     * Note that the returned signature is not qualified with the keyspace name.
     *
     * @return the signature of this function.
     */
    public String getSignature() {
        StringBuilder sb = new StringBuilder();
        sb
                .append(Metadata.escapeId(simpleName))
                .append('(');
        boolean first = true;
        for (DataType type : arguments.values()) {
            if (first)
                first = false;
            else
                sb.append(',');
            sb.append(type.asFunctionParameterString());
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * Returns the simple name of this function.
     * <p/>
     * This is the name of the function, without arguments. Note that functions can be overloaded with
     * different argument lists, therefore the simple name may not be unique. For example,
     * {@code sum(int,int)} and {@code sum(int,int,int)} both have the simple name {@code sum}.
     *
     * @return the simple name of this function.
     * @see #getSignature()
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
     * <p/>
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
        if (other == this)
            return true;

        if (other instanceof FunctionMetadata) {
            FunctionMetadata that = (FunctionMetadata) other;
            return this.keyspace.getName().equals(that.keyspace.getName()) &&
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
        return Objects.hashCode(keyspace.getName(), arguments, body, calledOnNullInput, language, returnType);
    }
}
