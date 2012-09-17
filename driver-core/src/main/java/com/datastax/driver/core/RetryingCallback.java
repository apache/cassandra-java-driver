package com.datastax.driver.core;

import com.datastax.driver.core.transport.*;
import com.datastax.driver.core.utils.SimpleFuture;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.exceptions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RetryingCallback implements Connection.ResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(RetryingCallback.class);

    private final Session.Manager manager;
    private final Message.Request request;
    private final Connection.ResponseCallback callback;

    private volatile int retries;

    public RetryingCallback(Session.Manager manager, Message.Request request, Connection.ResponseCallback callback) {
        this.manager = manager;
        this.request = request;
        this.callback = callback;
    }

    public Message.Request getRequest() {
        return request;
    }

    @Override
    public void onSet(Message.Response response) {
        switch (response.type) {
            case RESULT:
                callback.onSet(response);
                break;
            case ERROR:
                ErrorMessage err = (ErrorMessage)response;
                boolean retry = false;
                switch (err.error.code()) {
                    // TODO: Handle cases take into account by the retry policy
                    case READ_TIMEOUT:
                        assert err.error instanceof ReadTimeoutException;
                        ReadTimeoutException rte = (ReadTimeoutException)err.error;
                        ConsistencyLevel rcl = ConsistencyLevel.from(rte.consistency);
                        retry = manager.retryPolicy.onReadTimeout(rcl, rte.received, rte.blockFor, rte.dataPresent, retries);
                        break;
                    case WRITE_TIMEOUT:
                        assert err.error instanceof WriteTimeoutException;
                        WriteTimeoutException wte = (WriteTimeoutException)err.error;
                        ConsistencyLevel wcl = ConsistencyLevel.from(wte.consistency);
                        retry = manager.retryPolicy.onWriteTimeout(wcl, wte.received, wte.blockFor, retries);
                        break;
                    case UNAVAILABLE:
                        assert err.error instanceof UnavailableException;
                        UnavailableException ue = (UnavailableException)err.error;
                        ConsistencyLevel ucl = ConsistencyLevel.from(ue.consistency);
                        retry = manager.retryPolicy.onUnavailable(ucl, ue.required, ue.alive, retries);
                        break;
                    case OVERLOADED:
                        // TODO: maybe we could make that part of the retrying policy?
                        // retry once
                        if (retries == 0)
                            retry = true;
                        break;
                    case IS_BOOTSTRAPPING:
                        // TODO: log error as this shouldn't happen
                        // retry once
                        if (retries == 0)
                            retry = true;
                        break;
                }
                if (retry) {
                    ++retries;
                    manager.retry(this);
                } else {
                    callback.onSet(response);
                }

                break;
            default:
                callback.onSet(response);
                break;
        }
    }

    @Override
    public void onException(Exception exception) {
        callback.onException(exception);
    }
}
