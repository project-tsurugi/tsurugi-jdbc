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

import com.tsurugidb.jdbc.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionProperties;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSet;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSetConverter;
import com.tsurugidb.jdbc.resultset.TsurugiJdbcResultSetProperties;
import com.tsurugidb.jdbc.statement.TsurugiJdbcParameterGenerator;
import com.tsurugidb.jdbc.statement.TsurugiJdbcPreparedStatement;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatement;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatementProperties;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransaction;
import com.tsurugidb.jdbc.util.TsurugiJdbcConvertUtil;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class TsurugiJdbcFactory {

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

    public TsurugiJdbcConnection createConnection(Session lowSession, TsurugiJdbcProperties fromProperties) {
        var properties = TsurugiJdbcConnectionProperties.of(fromProperties);
        return createConnection(lowSession, properties);
    }

    public TsurugiJdbcConnection createConnection(Session lowSession, TsurugiJdbcConnectionProperties properties) {
        return new TsurugiJdbcConnection(this, lowSession, properties);
    }

    public TsurugiJdbcStatement createStatement(TsurugiJdbcConnection connection, TsurugiJdbcConnectionProperties fromProperties) {
        var properties = TsurugiJdbcStatementProperties.of(fromProperties);
        return createStatement(connection, properties);
    }

    public TsurugiJdbcStatement createStatement(TsurugiJdbcConnection connection, TsurugiJdbcStatementProperties properties) {
        return new TsurugiJdbcStatement(this, connection, properties);
    }

    public TsurugiJdbcPreparedStatement createPreparedStatement(TsurugiJdbcConnection connection, TsurugiJdbcConnectionProperties fromProperties, String sql) {
        var properties = TsurugiJdbcStatementProperties.of(fromProperties);
        return createPreparedStatement(connection, properties, sql);
    }

    public TsurugiJdbcPreparedStatement createPreparedStatement(TsurugiJdbcConnection connection, TsurugiJdbcStatementProperties properties, String sql) {
        return new TsurugiJdbcPreparedStatement(this, connection, properties, sql);
    }

    public TsurugiJdbcParameterGenerator createParameterGenerator(TsurugiJdbcPreparedStatement preparedStatement) {
        return new TsurugiJdbcParameterGenerator(preparedStatement);
    }

    public TsurugiJdbcTransaction createTransaction(Transaction lowTransaction, boolean autoCommit, TsurugiJdbcConnectionProperties properties) {
        return new TsurugiJdbcTransaction(this, lowTransaction, autoCommit, properties);
    }

    public TsurugiJdbcResultSet createResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<ResultSet> future, TsurugiJdbcStatementProperties fromProperties) {
        var properties = TsurugiJdbcResultSetProperties.of(fromProperties);
        return createResultSet(statement, transaction, future, properties);
    }

    public TsurugiJdbcResultSet createResultSet(TsurugiJdbcStatement statement, TsurugiJdbcTransaction transaction, FutureResponse<ResultSet> future, TsurugiJdbcResultSetProperties properties) {
        return new TsurugiJdbcResultSet(statement, transaction, future, properties);
    }

    public TsurugiJdbcResultSetConverter createResultSetConverter(HasFactory resultSet) {
        return new TsurugiJdbcResultSetConverter(resultSet);
    }

    public TsurugiJdbcConvertUtil createConvertUtil(HasFactory hasFactory) {
        return new TsurugiJdbcConvertUtil(hasFactory);
    }
}
