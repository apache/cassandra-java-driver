package com.datastax.driver.core.utils.querybuilder;

import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;

abstract class BuiltStatement extends Statement {

    private final List<ColumnMetadata> partitionKey;
    private final ByteBuffer[] routingKey;

    protected final StringBuilder builder = new StringBuilder();

    protected BuiltStatement() {
        this.partitionKey = null;
        this.routingKey = null;
    }

    protected BuiltStatement(TableMetadata tableMetadata) {
        this.partitionKey = tableMetadata.getPartitionKey();
        this.routingKey = new ByteBuffer[tableMetadata.getPartitionKey().size()];
    }

    public String getQueryString() {
        return builder.append(";").toString();
    }

    protected StringBuilder appendName(String name) {
        return Utils.appendName(name, builder);
    }

    // TODO: Correctly document the InvalidTypeException
    void maybeAddRoutingKey(String name, Object value) {
        if (routingKey == null || name == null)
            return;

        for (int i = 0; i < partitionKey.size(); i++) {
            if (name.equals(partitionKey.get(i).getName())) {
                routingKey[i] = partitionKey.get(i).getType().parse(Utils.toRawString(value));
                return;
            }
        }
    }

    public ByteBuffer getRoutingKey() {
        if (routingKey == null)
            return null;

        for (ByteBuffer bb : routingKey)
            if (bb == null)
                return null;

        return routingKey.length == 1
             ? routingKey[0]
             : compose(routingKey);
    }

    // This is a duplicate of the one in SimpleStatement, but I don't want to expose this publicly so...
    static ByteBuffer compose(ByteBuffer... buffers) {
        int totalLength = 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer bb : buffers)
        {
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte) 0);
        }
        out.flip();
        return out;
    }

    private static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }
}
