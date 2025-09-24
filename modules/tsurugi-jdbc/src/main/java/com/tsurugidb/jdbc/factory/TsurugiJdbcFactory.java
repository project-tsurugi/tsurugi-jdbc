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
package com.tsurugidb.jdbc.factory;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionConfig;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.resultset.AbstractResultSet;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSetConfig;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSetConverter;
import com.tsurugidb.jdbc.statement.TsurugiJdbcParameterGenerator;
import com.tsurugidb.jdbc.statement.TsurugiJdbcParameterMetaData;
import com.tsurugidb.jdbc.statement.TsurugiJdbcPreparedStatement;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatement;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatementConfig;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransaction;
import com.tsurugidb.jdbc.util.TsurugiJdbcConvertUtil;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class TsurugiJdbcFactory {

    private static TsurugiJdbcFactory defaultFactory = new TsurugiJdbcFactory();

    public static void setDefaultFactory(@Nonnull TsurugiJdbcFactory factory) {
        defaultFactory = Objects.requireNonNull(factory);
    }

    public static TsurugiJdbcFactory getDefaultFactory() {
        return defaultFactory;
    }

    private TsurugiJdbcExceptionHandler exceptionHandler = new TsurugiJdbcExceptionHandler();
    private TsurugiJdbcSqlTypeUtil sqlTypeUtil = new TsurugiJdbcSqlTypeUtil();

    public void setExceptionHandler(@Nonnull TsurugiJdbcExceptionHandler exceptionHandler) {
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
    }

    public TsurugiJdbcExceptionHandler getExceptionHandler() {
        return this.exceptionHandler;
    }

    public void setSqlTypeUtil(@Nonnull TsurugiJdbcSqlTypeUtil sqlTypeUtil) {
        this.sqlTypeUtil = Objects.requireNonNull(sqlTypeUtil);
    }

    public TsurugiJdbcSqlTypeUtil getSqlTypeUtil() {
        return this.sqlTypeUtil;
    }

    public TsurugiJdbcConnection createConnection(Session lowSession, TsurugiConfig fromConfig) {
        var config = TsurugiJdbcConnectionConfig.of(fromConfig);
        return createConnection(lowSession, config);
    }

    public TsurugiJdbcConnection createConnection(Session lowSession, TsurugiJdbcConnectionConfig config) {
        return new TsurugiJdbcConnection(this, lowSession, config);
    }

    public TsurugiJdbcStatement createStatement(TsurugiJdbcConnection connection, TsurugiJdbcConnectionConfig fromConfig) {
        var config = TsurugiJdbcStatementConfig.of(fromConfig);
        return createStatement(connection, config);
    }

    public TsurugiJdbcStatement createStatement(TsurugiJdbcConnection connection, TsurugiJdbcStatementConfig config) {
        return new TsurugiJdbcStatement(this, connection, config);
    }

    public TsurugiJdbcPreparedStatement createPreparedStatement(TsurugiJdbcConnection connection, TsurugiJdbcConnectionConfig fromConfig, String sql) {
        var config = TsurugiJdbcStatementConfig.of(fromConfig);
        return createPreparedStatement(connection, config, sql);
    }

    public TsurugiJdbcPreparedStatement createPreparedStatement(TsurugiJdbcConnection connection, TsurugiJdbcStatementConfig config, String sql) {
        return new TsurugiJdbcPreparedStatement(this, connection, config, sql);
    }

    public TsurugiJdbcParameterGenerator createParameterGenerator(TsurugiJdbcPreparedStatement preparedStatement) {
        return new TsurugiJdbcParameterGenerator(preparedStatement);
    }

    public TsurugiJdbcParameterMetaData createParameterMetaDate(TsurugiJdbcPreparedStatement preparedStatement) {
        return new TsurugiJdbcParameterMetaData(preparedStatement);
    }

    public TsurugiJdbcTransaction createTransaction(Transaction lowTransaction, boolean autoCommit, TsurugiJdbcConnectionConfig config) {
        return new TsurugiJdbcTransaction(this, lowTransaction, autoCommit, config);
    }

    public TsurugiJdbcResultSet createResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<ResultSet> future, TsurugiJdbcStatementConfig fromConfig) {
        var config = TsurugiJdbcResultSetConfig.of(fromConfig);
        return createResultSet(statement, transaction, future, config);
    }

    public TsurugiJdbcResultSet createResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<ResultSet> future, TsurugiJdbcResultSetConfig config) {
        return new TsurugiJdbcResultSet(statement, transaction, future, config);
    }

    public TsurugiJdbcResultSetConverter createResultSetConverter(AbstractResultSet resultSet) {
        return new TsurugiJdbcResultSetConverter(resultSet);
    }

    public TsurugiJdbcConvertUtil createConvertUtil(GetFactory factoryHolder) {
        return new TsurugiJdbcConvertUtil(factoryHolder);
    }
}
