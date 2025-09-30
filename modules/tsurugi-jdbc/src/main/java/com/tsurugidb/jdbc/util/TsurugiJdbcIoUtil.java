/*
 * Copyright 2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.jdbc.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.GetFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.Timeout;
import com.tsurugidb.tsubakuro.util.Timeout.Policy;

/**
 * Tsurugi JDBC I/O utility.
 */
@TsurugiJdbcInternal
public class TsurugiJdbcIoUtil implements GetFactory {

    private final TsurugiJdbcFactory factory;

    /**
     * Creates a new instance.
     *
     * @param factory factory holder
     */
    public TsurugiJdbcIoUtil(TsurugiJdbcFactory factory) {
        this.factory = factory;
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        return factory;
    }

    /**
     * Get exception handler.
     *
     * @return exception handler
     */
    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    /**
     * Get value from FutureResponse.
     *
     * @param <V>     the result value type
     * @param future  FutureResponse
     * @param timeout timeout [seconds]
     * @return return value
     * @throws IOException          if exception was occurred while communicating to the server
     * @throws InterruptedException if interrupted from other threads while waiting for response
     * @throws ServerException      if exception was occurred while processing the request in the server
     * @throws TimeoutException     if the wait time out
     */
    public <V> V get(FutureResponse<V> future, int timeout) throws IOException, InterruptedException, ServerException, TimeoutException {
        return get(future, timeout, TimeUnit.SECONDS);
    }

    /**
     * Get value from FutureResponse.
     *
     * @param <V>     the result value type
     * @param future  FutureResponse
     * @param timeout timeout
     * @param unit    time unit
     * @return return value
     * @throws IOException          if exception was occurred while communicating to the server
     * @throws InterruptedException if interrupted from other threads while waiting for response
     * @throws ServerException      if exception was occurred while processing the request in the server
     * @throws TimeoutException     if the wait time out
     */
    public <V> V get(FutureResponse<V> future, long timeout, TimeUnit unit) throws IOException, InterruptedException, ServerException, TimeoutException {
        return future.await(timeout, unit);
    }

    /**
     * Close ResultSet.
     *
     * @param resultSetFuture FutureResponse of ResultSet
     * @param lowResultSet    ResultSet
     * @param timeout         timeout [seconds]
     * @throws IOException          if exception was occurred while communicating to the server
     * @throws InterruptedException if interrupted from other threads while waiting for response
     * @throws ServerException      if exception was occurred while processing the request in the server
     * @throws TimeoutException     if the wait time out
     */
    public void close(@Nullable FutureResponse<ResultSet> resultSetFuture, @Nullable ResultSet lowResultSet, int timeout) throws IOException, InterruptedException, ServerException, TimeoutException {
        if (lowResultSet == null) {
            if (resultSetFuture != null) {
                // TODO WORKAROUND: remove future.await()
                try (var rs = resultSetFuture.await(timeout, TimeUnit.SECONDS)) {
                    rs.setCloseTimeout(new Timeout(timeout, TimeUnit.SECONDS, Policy.ERROR));
                }
            }
        } else {
            try (resultSetFuture; lowResultSet) {
                lowResultSet.setCloseTimeout(new Timeout(timeout, TimeUnit.SECONDS, Policy.ERROR));
            }
        }
    }
}
