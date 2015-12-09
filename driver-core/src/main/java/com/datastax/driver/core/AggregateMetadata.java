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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * Describes a CQL aggregate function (created with {@code CREATE AGGREGATE...}).
 */
public class AggregateMetadata {

    private final KeyspaceMetadata keyspace;
    private final String simpleName;
    private final List<DataType> argumentTypes;
    private final String finalFuncSimpleName;
    private final String finalFuncFullName;
    private final Object initCond;
    private final DataType returnType;
    private final String stateFuncSimpleName;
    private final String stateFuncFullName;
    private final DataType stateType;
    private final TypeCodec<Object> stateTypeCodec;

    private AggregateMetadata(KeyspaceMetadata keyspace, String simpleName, List<DataType> argumentTypes,
                              String finalFuncSimpleName, String finalFuncFullName, Object initCond, DataType returnType,
                              String stateFuncSimpleName, String stateFuncFullName, DataType stateType, TypeCodec<Object> stateTypeCodec) {
        this.keyspace = keyspace;
        this.simpleName = simpleName;
        this.argumentTypes = argumentTypes;
        this.finalFuncSimpleName = finalFuncSimpleName;
        this.finalFuncFullName = finalFuncFullName;
        this.initCond = initCond;
        this.returnType = returnType;
        this.stateFuncSimpleName = stateFuncSimpleName;
        this.stateFuncFullName = stateFuncFullName;
        this.stateType = stateType;
        this.stateTypeCodec = stateTypeCodec;
    }

    // Cassandra < 3.0:
    // CREATE TABLE system.schema_aggregates (
    //     keyspace_name text,
    //     aggregate_name text,
    //     signature frozen<list<text>>,
    //     argument_types list<text>,
    //     final_func text,
    //     initcond blob,
    //     return_type text,
    //     state_func text,
    //     state_type text,
    //     PRIMARY KEY (keyspace_name, aggregate_name, signature)
    // ) WITH CLUSTERING ORDER BY (aggregate_name ASC, signature ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system.schema_aggregates (
    //     keyspace_name text,
    //     aggregate_name text,
    //     argument_types frozen<list<text>>,
    //     final_func text,
    //     initcond text,
    //     return_type text,
    //     state_func text,
    //     state_type text,
    //     PRIMARY KEY (keyspace_name, aggregate_name, argument_types)
    // ) WITH CLUSTERING ORDER BY (aggregate_name ASC, argument_types ASC)
    static AggregateMetadata build(KeyspaceMetadata ksm, Row row, VersionNumber version, Cluster cluster) {
        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        String simpleName = row.getString("aggregate_name");
        List<DataType> argumentTypes = parseTypes(ksm, row.getList("argument_types", String.class), version, cluster);
        String finalFuncSimpleName = row.getString("final_func");
        DataType returnType;
        if (version.getMajor() >= 3) {
            returnType = DataTypeCqlNameParser.parse(row.getString("return_type"), cluster, ksm.getName(), ksm.userTypes, null, false, false);
        } else {
            returnType = DataTypeClassNameParser.parseOne(row.getString("return_type"), protocolVersion, codecRegistry);
        }
        String stateFuncSimpleName = row.getString("state_func");
        String stateTypeName = row.getString("state_type");
        DataType stateType;
        Object initCond;
        if (version.getMajor() >= 3) {
            stateType = DataTypeCqlNameParser.parse(stateTypeName, cluster, ksm.getName(), ksm.userTypes, null, false, false);
            String rawInitCond = row.getString("initcond");
            initCond = rawInitCond == null ? null : codecRegistry.codecFor(stateType).parse(rawInitCond);
        } else {
            stateType = DataTypeClassNameParser.parseOne(stateTypeName, protocolVersion, codecRegistry);
            ByteBuffer rawInitCond = row.getBytes("initcond");
            initCond = rawInitCond == null ? null : codecRegistry.codecFor(stateType).deserialize(rawInitCond, protocolVersion);
        }

        String finalFuncFullName = finalFuncSimpleName == null ? null : Metadata.fullFunctionName(finalFuncSimpleName, Collections.singletonList(stateType));
        String stateFuncFullName = makeStateFuncFullName(stateFuncSimpleName, stateType, argumentTypes);

        return new AggregateMetadata(ksm, simpleName, argumentTypes,
                finalFuncSimpleName, finalFuncFullName, initCond, returnType, stateFuncSimpleName,
                stateFuncFullName, stateType, codecRegistry.codecFor(stateType));
    }

    private static String makeStateFuncFullName(String stateFuncSimpleName, DataType stateType, List<DataType> argumentTypes) {
        List<DataType> args = Lists.newArrayList(stateType);
        args.addAll(argumentTypes);
        return Metadata.fullFunctionName(stateFuncSimpleName, args);
    }

    private static List<DataType> parseTypes(KeyspaceMetadata ksm, List<String> types, VersionNumber version, Cluster cluster) {
        if (types.isEmpty())
            return Collections.emptyList();

        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        ImmutableList.Builder<DataType> builder = ImmutableList.builder();
        for (String name : types) {
            DataType type;
            if (version.getMajor() >= 3) {
                type = DataTypeCqlNameParser.parse(name, cluster, ksm.getName(), ksm.userTypes, null, false, false);
            } else {
                type = DataTypeClassNameParser.parseOne(name, protocolVersion, codecRegistry);
            }
            builder.add(type);
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

        StringBuilder sb = new StringBuilder("CREATE AGGREGATE ")
                .append(Metadata.escapeId(keyspace.getName()))
                .append('.');

        appendSignature(sb);

        TableMetadata.spaceOrNewLine(sb, formatted)
                .append("SFUNC ")
                .append(Metadata.escapeId(stateFuncSimpleName))
                .append(" STYPE ")
                .append(stateType.asFunctionParameterString());

        if (finalFuncSimpleName != null)
            TableMetadata.spaceOrNewLine(sb, formatted)
                    .append("FINALFUNC ")
                    .append(Metadata.escapeId(finalFuncSimpleName));

        if (initCond != null)
            TableMetadata.spaceOrNewLine(sb, formatted)
                    .append("INITCOND ")
                    .append(stateTypeCodec.format(initCond));

        sb.append(';');

        return sb.toString();
    }

    private void appendSignature(StringBuilder sb) {
        sb
                .append(Metadata.escapeId(simpleName))
                .append('(');
        boolean first = true;
        for (DataType type : argumentTypes) {
            if (first)
                first = false;
            else
                sb.append(',');
            sb.append(type.asFunctionParameterString());
        }
        sb.append(')');
    }

    /**
     * Returns the keyspace this aggregate belongs to.
     *
     * @return the keyspace metadata of the keyspace this aggregate belongs to.
     */
    public KeyspaceMetadata getKeyspace() {
        return keyspace;
    }

    /**
     * Returns the CQL signature of this aggregate.
     * <p/>
     * This is the name of the aggregate, followed by the names of the argument types between parentheses,
     * like it was specified in the {@code CREATE AGGREGATE...} statement, for example {@code sum(int)}.
     * <p/>
     * Note that the returned signature is not qualified with the keyspace name.
     *
     * @return the signature of this aggregate.
     */
    public String getSignature() {
        StringBuilder sb = new StringBuilder();
        appendSignature(sb);
        return sb.toString();
    }

    /**
     * Returns the simple name of this aggregate.
     * <p/>
     * This is the name of the aggregate, without arguments. Note that aggregates can be overloaded with
     * different argument lists, therefore the simple name may not be unique. For example,
     * {@code sum(int)} and {@code sum(int,int)} both have the simple name {@code sum}.
     *
     * @return the simple name of this aggregate.
     * @see #getSignature()
     */
    public String getSimpleName() {
        return simpleName;
    }

    /**
     * Returns the types of this aggregate's arguments.
     *
     * @return the types.
     */
    public List<DataType> getArgumentTypes() {
        return argumentTypes;
    }

    /**
     * Returns the final function of this aggregate.
     * <p/>
     * This is the function specified with {@code FINALFUNC} in the {@code CREATE AGGREGATE...}
     * statement. It transforms the final value after the aggregation is complete.
     *
     * @return the metadata of the final function, or {@code null} if there is none.
     */
    public FunctionMetadata getFinalFunc() {
        return (finalFuncFullName == null)
                ? null
                : keyspace.functions.get(finalFuncFullName);
    }

    /**
     * Returns the initial state value of this aggregate.
     * <p/>
     * This is the value specified with {@code INITCOND} in the {@code CREATE AGGREGATE...}
     * statement. It's passed to the initial invocation of the state function (if that function
     * does not accept null arguments).
     *
     * @return the initial state, or {@code null} if there is none.
     */
    public Object getInitCond() {
        return initCond;
    }

    /**
     * Returns the return type of this aggregate.
     * <p/>
     * This is the final type of the value computed by this aggregate; in other words, the return
     * type of the final function if it is defined, or the state type otherwise.
     *
     * @return the return type.
     */
    public DataType getReturnType() {
        return returnType;
    }

    /**
     * Returns the state function of this aggregate.
     * <p/>
     * This is the function specified with {@code SFUNC} in the {@code CREATE AGGREGATE...}
     * statement. It aggregates the current state with each row to produce a new state.
     *
     * @return the metadata of the state function.
     */
    public FunctionMetadata getStateFunc() {
        return keyspace.functions.get(stateFuncFullName);
    }

    /**
     * Returns the state type of this aggregate.
     * <p/>
     * This is the type specified with {@code STYPE} in the {@code CREATE AGGREGATE...}
     * statement. It defines the type of the value that is accumulated as the aggregate
     * iterates through the rows.
     *
     * @return the state type.
     */
    public DataType getStateType() {
        return stateType;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;

        if (other instanceof AggregateMetadata) {
            AggregateMetadata that = (AggregateMetadata) other;
            return this.keyspace.getName().equals(that.keyspace.getName()) &&
                    this.argumentTypes.equals(that.argumentTypes) &&
                    Objects.equal(this.finalFuncFullName, that.finalFuncFullName) &&
                    // Note: this might be a problem if a custom codec has been registered for the initCond's type, with a target Java type that
                    // does not properly implement equals. We don't have any control over this, at worst this would lead to spurious change
                    // notifications.
                    Objects.equal(this.initCond, that.initCond) &&
                    this.returnType.equals(that.returnType) &&
                    this.stateFuncFullName.equals(that.stateFuncFullName) &&
                    this.stateType.equals(that.stateType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.keyspace.getName(), this.argumentTypes,
                this.finalFuncFullName, this.initCond, this.returnType, this.stateFuncFullName, this.stateType);
    }
}
