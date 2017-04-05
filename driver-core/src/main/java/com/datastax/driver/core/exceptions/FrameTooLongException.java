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
package com.datastax.driver.core.exceptions;

/**
 * Indicates that the response frame for a request exceeded
 * {@link com.datastax.driver.core.Frame.Decoder.DecoderForStreamIdSize#MAX_FRAME_LENGTH}
 * (default: 256MB, configurable via com.datastax.driver.NATIVE_TRANSPORT_MAX_FRAME_SIZE_IN_MB
 * system property) and thus was not parsed.
 */
public class FrameTooLongException extends DriverException {

    private static final long serialVersionUID = 0;

    private final int streamId;

    public FrameTooLongException(int streamId) {
        this(streamId, null);
    }

    private FrameTooLongException(int streamId, Throwable cause) {
        super("Response frame exceeded maximum allowed length", cause);
        this.streamId = streamId;
    }

    /**
     * @return The stream id associated with the frame that caused this exception.
     */
    public int getStreamId() {
        return streamId;
    }

    @Override
    public FrameTooLongException copy() {
        return new FrameTooLongException(streamId, this);
    }
}
