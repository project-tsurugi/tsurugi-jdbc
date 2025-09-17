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
package com.tsurugidb.jdbc.connection;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatement;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransaction;
import com.tsurugidb.jdbc.util.LowCloser;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;

public class TsurugiJdbcConnection implements Connection, HasFactory {
    private static final Logger LOG = Logger.getLogger(TsurugiJdbcConnection.class.getName());

    private TsurugiJdbcFactory factory;
    private final Session lowSession;
    private final SqlClient lowSqlClient;
    private final TsurugiJdbcConnectionProperties properties;

    private TsurugiJdbcTransaction transaction = null;

    @TsurugiJdbcInternal
    public TsurugiJdbcConnection(TsurugiJdbcFactory factory, Session lowSession, TsurugiJdbcConnectionProperties properties) {
        this.factory = factory;
        this.lowSession = Objects.requireNonNull(lowSession);
        this.lowSqlClient = SqlClient.attach(lowSession);
        this.properties = properties;
    }

    @Override
    public void setFactory(TsurugiJdbcFactory factory) {
        this.factory = factory;
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        return this.factory;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw factory.getExceptionHandler().unwrapException(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public TsurugiJdbcStatement createStatement() throws SQLException {
        return factory.createStatement(this, properties);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return factory.createPreparedStatement(this, properties, sql);
    }

    @Override
    @TsurugiJdbcNotSupported
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("prepareCall not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @TsurugiJdbcInternal
    public SqlClient getLowSqlClient() {
        return this.lowSqlClient;
    }

    public void setTransactionType(TsurugiJdbcTransactionType type) {
        properties.setTransactionType(type);
    }

    public TsurugiJdbcTransactionType getTransactionType() {
        return properties.getTransactionType();
    }

    @TsurugiJdbcInternal
    public synchronized TsurugiJdbcTransaction getTransaction() throws SQLException {
        var transaction = getFieldTransaction();
        if (transaction != null) {
            return transaction;
        }

        var option = getTransactionOption();
        LOG.config(() -> String.format("transactionOption=%s", option));

        int timeout = properties.getBeginTimeout();
        LOG.config(() -> String.format("beginTimeout=%d [seconds]", timeout));

        Transaction lowTransaction;
        try {
            lowTransaction = lowSqlClient.createTransaction(option).await(timeout, TimeUnit.SECONDS);
        } catch (IOException | ServerException | InterruptedException | TimeoutException e) {
            throw factory.getExceptionHandler().sqlException("Transaction create error", e);
        }

        transaction = factory.createTransaction(lowTransaction, getAutoCommit(), properties);
        this.transaction = transaction;
        return transaction;
    }

    protected SqlRequest.TransactionOption getTransactionOption() {
        return properties.getTransactionOption();
    }

    protected TsurugiJdbcTransaction checkTransactionActive() throws SQLException {
        var transaction = getFieldTransaction();
        if (transaction == null) {
            throw factory.getExceptionHandler().transactionNotFoundException();
        }
        return transaction;
    }

    protected void checkTransactionInactive() throws SQLException {
        var transaction = getFieldTransaction();
        if (transaction != null) {
            throw factory.getExceptionHandler().transactionFoundException();
        }
    }

    private synchronized TsurugiJdbcTransaction getFieldTransaction() {
        var transaction = this.transaction;
        if (transaction != null && transaction.isClosed()) {
            this.transaction = null;
            return null;
        }
        return transaction;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        properties.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return properties.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        var transaction = checkTransactionActive();
        try {
            transaction.commit();
        } finally {
            this.transaction = null;
        }
    }

    @Override
    public void rollback() throws SQLException {
        var transaction = checkTransactionActive();
        try {
            transaction.rollback();
        } finally {
            this.transaction = null;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        setTransactionType(TsurugiJdbcTransactionType.RTX);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getTransactionType() == TsurugiJdbcTransactionType.RTX;
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setCatalog(String catalog) throws SQLException {
        // not supported
    }

    @Override
    @TsurugiJdbcNotSupported
    public String getCatalog() throws SQLException {
        return null; // not supported
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // Tsurugi is TRANSACTION_SERIALIZABLE only
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_SERIALIZABLE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetType");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetConcurrency");
        }

        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetType");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetConcurrency");
        }

        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    @TsurugiJdbcNotSupported
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException("Unsupported holdability");
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    @TsurugiJdbcNotSupported
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback(Savepoint) not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetType");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetConcurrency");
        }
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException("Unsupported holdability");
        }

        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetType");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Unsupported resultSetConcurrency");
        }
        if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException("Unsupported holdability");
        }

        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException("Unsupported autoGeneratedKeys");
        }

        return prepareStatement(sql);
    }

    @Override
    @TsurugiJdbcNotSupported
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(columnIndexes) not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(columnNames) not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return lowSession.isAlive();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        var failedProperties = new LinkedHashMap<String, ClientInfoStatus>();

        try {
            properties.put(name, value, failedProperties);
        } catch (Exception e) {
            throw factory.getExceptionHandler().clientInfoException(e, failedProperties);
        }

        if (!failedProperties.isEmpty()) {
            throw factory.getExceptionHandler().clientInfoException(null, failedProperties);
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        var failedProperties = new LinkedHashMap<String, ClientInfoStatus>();
        var exceptionList = new ArrayList<Exception>();

        for (var entry : properties.entrySet()) {
            try {
                String key = (String) entry.getKey();
                String value = (String) entry.getValue();
                this.properties.put(key, value, failedProperties);
            } catch (Exception e) {
                exceptionList.add(e);
            }
        }

        if (!failedProperties.isEmpty()) {
            var firstException = !exceptionList.isEmpty() ? exceptionList.get(0) : null;
            var e = factory.getExceptionHandler().clientInfoException(firstException, failedProperties);
            for (int i = 1; i < exceptionList.size(); i++) {
                e.addSuppressed(exceptionList.get(i));
            }
            throw e;
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        var property = properties.getInternalProperties().getProperty(name);
        if (property == null) {
            return null;
        }
        return property.getStringValue();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        var entrySet = properties.getInternalProperties().getProperties();
        var result = new Properties(entrySet.size());
        for (var entry : entrySet) {
            String key = entry.getKey();
            var property = entry.getValue();
            String value = (property != null) ? property.getStringValue() : null;
            result.setProperty(key, value);
        }
        return result;
    }

    @Override
    @TsurugiJdbcNotSupported
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setSchema(String schema) throws SQLException {
        // not supported
    }

    @Override
    @TsurugiJdbcNotSupported
    public String getSchema() throws SQLException {
        return null; // not supported
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() throws SQLException {
        LowCloser shutdown;
        {
            var shutdownType = properties.getShutdownType();
            LOG.config(() -> String.format("shutdownType=%s", shutdownType));

            var lowShutdownType = shutdownType.getLowShutdownType();
            if (lowShutdownType != null) {
                int timeout = properties.getShutdownTimeout();
                LOG.config(() -> String.format("shutdownTimeout=%d [seconds]", timeout));

                shutdown = () -> {
                    lowSession.shutdown(lowShutdownType).await(timeout, TimeUnit.SECONDS);
                };
            } else {
                shutdown = null;
            }
        }

        try (var s = lowSession; shutdown; var c = lowSqlClient; var t = transaction) {
            this.transaction = null;
        } catch (ServerException | IOException | InterruptedException | TimeoutException e) {
            throw factory.getExceptionHandler().sqlException("Connection close error", e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return lowSession.isClosed();
    }
}
