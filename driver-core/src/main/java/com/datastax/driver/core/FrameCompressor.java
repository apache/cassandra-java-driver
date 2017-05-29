/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

abstract class FrameCompressor {

    abstract Frame compress(Frame frame) throws IOException;

    abstract Frame decompress(Frame frame) throws IOException;

    protected static ByteBuffer inputNioBuffer(ByteBuf buf) {
        // Using internalNioBuffer(...) as we only hold the reference in this method and so can
        // reduce Object allocations.
        int index = buf.readerIndex();
        int len = buf.readableBytes();
        return buf.nioBufferCount() == 1 ? buf.internalNioBuffer(index, len) : buf.nioBuffer(index, len);
    }

    protected static ByteBuffer outputNioBuffer(ByteBuf buf) {
        int index = buf.writerIndex();
        int len = buf.writableBytes();
        return buf.nioBufferCount() == 1 ? buf.internalNioBuffer(index, len) : buf.nioBuffer(index, len);
    }
}
