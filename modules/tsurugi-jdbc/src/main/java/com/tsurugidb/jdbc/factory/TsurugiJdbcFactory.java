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
import com.tsurugidb.jdbc.util.TsurugiJdbcIoUtil;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi JDBC Factory to create various objects.
 */
public class TsurugiJdbcFactory {

    private static TsurugiJdbcFactory defaultFactory = new TsurugiJdbcFactory();

    /**
     * Set the default factory.
     *
     * @param factory factory
     */
    public static void setDefaultFactory(@Nonnull TsurugiJdbcFactory factory) {
        defaultFactory = Objects.requireNonNull(factory);
    }

    /**
     * Get the default factory.
     *
     * @return factory
     */
    public static TsurugiJdbcFactory getDefaultFactory() {
        return defaultFactory;
    }

    private TsurugiJdbcExceptionHandler exceptionHandler = new TsurugiJdbcExceptionHandler();
    private TsurugiJdbcSqlTypeUtil sqlTypeUtil = new TsurugiJdbcSqlTypeUtil();
    private TsurugiJdbcIoUtil ioUtil = new TsurugiJdbcIoUtil(this);

    /**
     * Set exception handler.
     *
     * @param exceptionHandler exception handler
     */
    public void setExceptionHandler(@Nonnull TsurugiJdbcExceptionHandler exceptionHandler) {
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
    }

    /**
     * Get exception handler.
     *
     * @return exception handler
     */
    public TsurugiJdbcExceptionHandler getExceptionHandler() {
        return this.exceptionHandler;
    }

    /**
     * Set SQL type utility.
     *
     * @param sqlTypeUtil SQL type utility
     */
    public void setSqlTypeUtil(@Nonnull TsurugiJdbcSqlTypeUtil sqlTypeUtil) {
        this.sqlTypeUtil = Objects.requireNonNull(sqlTypeUtil);
    }

    /**
     * Get SQL type utility.
     *
     * @return SQL type utility
     */
    public TsurugiJdbcSqlTypeUtil getSqlTypeUtil() {
        return this.sqlTypeUtil;
    }

    /**
     * Create Tsurugi JDBC connection.
     *
     * @param lowSession session
     * @param fromConfig Tsurugi JDBC configuration
     * @return connection
     */
    public TsurugiJdbcConnection createConnection(Session lowSession, TsurugiConfig fromConfig) {
        var config = TsurugiJdbcConnectionConfig.of(fromConfig);
        return createConnection(lowSession, config);
    }

    /**
     * Create Tsurugi JDBC connection.
     *
     * @param lowSession session
     * @param config     connection configuration
     * @return connection
     */
    public TsurugiJdbcConnection createConnection(Session lowSession, TsurugiJdbcConnectionConfig config) {
        return new TsurugiJdbcConnection(this, lowSession, config);
    }

    /**
     * Create Tsurugi JDBC statement.
     *
     * @param connection connection
     * @param fromConfig connection configuration
     * @return statement
     */
    public TsurugiJdbcStatement createStatement(TsurugiJdbcConnection connection, TsurugiJdbcConnectionConfig fromConfig) {
        var config = TsurugiJdbcStatementConfig.of(fromConfig);
        return createStatement(connection, config);
    }

    /**
     * Create Tsurugi JDBC statement.
     *
     * @param connection connection
     * @param config     statement configuration
     * @return statement
     */
    public TsurugiJdbcStatement createStatement(TsurugiJdbcConnection connection, TsurugiJdbcStatementConfig config) {
        return new TsurugiJdbcStatement(this, connection, config);
    }

    /**
     * Create Tsurugi JDBC prepared statement.
     *
     * @param connection connection
     * @param fromConfig connection configuration
     * @param sql        SQL query
     * @return prepared statement
     */
    public TsurugiJdbcPreparedStatement createPreparedStatement(TsurugiJdbcConnection connection, TsurugiJdbcConnectionConfig fromConfig, String sql) {
        var config = TsurugiJdbcStatementConfig.of(fromConfig);
        return createPreparedStatement(connection, config, sql);
    }

    /**
     * Create Tsurugi JDBC prepared statement.
     *
     * @param connection connection
     * @param config     statement configuration
     * @param sql        SQL query
     * @return prepared statement
     */
    public TsurugiJdbcPreparedStatement createPreparedStatement(TsurugiJdbcConnection connection, TsurugiJdbcStatementConfig config, String sql) {
        return new TsurugiJdbcPreparedStatement(this, connection, config, sql);
    }

    /**
     * Create parameter generator.
     *
     * @param preparedStatement prepared statement
     * @return parameter generator
     */
    public TsurugiJdbcParameterGenerator createParameterGenerator(TsurugiJdbcPreparedStatement preparedStatement) {
        return new TsurugiJdbcParameterGenerator(preparedStatement);
    }

    /**
     * Create parameter meta data.
     *
     * @param preparedStatement prepared statement
     * @return parameter meta data
     */
    public TsurugiJdbcParameterMetaData createParameterMetaDate(TsurugiJdbcPreparedStatement preparedStatement) {
        return new TsurugiJdbcParameterMetaData(preparedStatement);
    }

    /**
     * Create transaction.
     *
     * @param lowTransaction low-level transaction
     * @param autoCommit     auto commit
     * @param config         connection configuration
     * @return transaction
     */
    public TsurugiJdbcTransaction createTransaction(Transaction lowTransaction, boolean autoCommit, TsurugiJdbcConnectionConfig config) {
        return new TsurugiJdbcTransaction(this, lowTransaction, autoCommit, config);
    }

    /**
     * Create JDBC result set.
     *
     * @param statement   prepared statement
     * @param transaction transaction
     * @param future      future of result set
     * @param fromConfig  statement configuration
     * @return result set
     */
    public TsurugiJdbcResultSet createResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<ResultSet> future, TsurugiJdbcStatementConfig fromConfig) {
        var config = TsurugiJdbcResultSetConfig.of(fromConfig);
        return createResultSet(statement, transaction, future, config);
    }

    /**
     * Create JDBC result set.
     *
     * @param statement   prepared statement
     * @param transaction transaction
     * @param future      future of result set
     * @param config      result set configuration
     * @return result set
     */
    public TsurugiJdbcResultSet createResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<ResultSet> future, TsurugiJdbcResultSetConfig config) {
        return new TsurugiJdbcResultSet(statement, transaction, future, config);
    }

    /**
     * Create result set converter.
     *
     * @param resultSet result set
     * @return result set converter
     */
    public TsurugiJdbcResultSetConverter createResultSetConverter(AbstractResultSet resultSet) {
        return new TsurugiJdbcResultSetConverter(resultSet);
    }

    /**
     * Create convert utility.
     *
     * @param factoryHolder factory holder
     * @return convert utility
     */
    public TsurugiJdbcConvertUtil createConvertUtil(GetFactory factoryHolder) {
        return new TsurugiJdbcConvertUtil(factoryHolder);
    }

    /**
     * Create I/O utility.
     *
     * @return I/O utility
     */
    public TsurugiJdbcIoUtil getIoUtil() {
        return this.ioUtil;
    }
}
