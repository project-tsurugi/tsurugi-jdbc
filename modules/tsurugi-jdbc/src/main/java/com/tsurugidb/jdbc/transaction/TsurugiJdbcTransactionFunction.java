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
package com.tsurugidb.jdbc.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Transaction;

/**
 * Tsurugi JDBC Transaction Function.
 *
 * @param <R> return type
 */
@FunctionalInterface
@TsurugiJdbcInternal
public interface TsurugiJdbcTransactionFunction<R> {

    /**
     * Execute action.
     *
     * @param lowTransaction low-level transaction
     * @return result
     * @throws SQLException         if any error occurs
     * @throws IOException          if exception was occurred while communicating to the server
     * @throws InterruptedException if interrupted from other threads while waiting for response
     * @throws ServerException      if exception was occurred while processing the request in the server
     * @throws TimeoutException     if the wait time out
     */
    public R execute(Transaction lowTransaction) throws SQLException, IOException, InterruptedException, ServerException, TimeoutException;
}
