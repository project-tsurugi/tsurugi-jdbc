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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.statement.TsurugiJdbcStatement;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransaction;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;
import com.tsurugidb.jdbc.util.LowCloser;
import com.tsurugidb.sql.proto.SqlRequest;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;

/**
 * Tsurugi JDBC Connection.
 */
public class TsurugiJdbcConnection implements Connection, HasFactory {
    private static final Logger LOG = Logger.getLogger(TsurugiJdbcConnection.class.getName());

    private TsurugiJdbcFactory factory;
    private final Session lowSession;
    private final SqlClient lowSqlClient;
    private final TsurugiJdbcConnectionConfig config;

    private TsurugiJdbcDatabaseMetaData metaData = null;

    private TsurugiJdbcTransaction transaction = null;

    /**
     * Creates a new instance.
     *
     * @param factory    factory
     * @param lowSession low-level session
     * @param config     connection configuration
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcConnection(TsurugiJdbcFactory factory, Session lowSession, TsurugiJdbcConnectionConfig config) {
        this.factory = Objects.requireNonNull(factory, "factory is null");
        this.lowSession = Objects.requireNonNull(lowSession);
        this.lowSqlClient = SqlClient.attach(lowSession);
        this.config = config;
    }

    @Override
    public void setFactory(TsurugiJdbcFactory factory) {
        this.factory = Objects.requireNonNull(factory, "factory is null");
    }

    @Override
    public TsurugiJdbcFactory getFactory() {
        return this.factory;
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
     * Get configuration.
     *
     * @return connection configuration
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcConnectionConfig getConfig() {
        return this.config;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw getExceptionHandler().unwrapException(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public TsurugiJdbcStatement createStatement() throws SQLException {
        return factory.createStatement(this, config);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return factory.createPreparedStatement(this, config, sql);
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

    /**
     * Get low-level session.
     *
     * @return low-level session
     */
    @TsurugiJdbcInternal
    public Session getLowSession() {
        return this.lowSession;
    }

    /**
     * Get low-level SQL client.
     *
     * @return low-level SQL client
     */
    @TsurugiJdbcInternal
    public SqlClient getLowSqlClient() {
        return this.lowSqlClient;
    }

    /**
     * Set transaction type.
     *
     * @param type transaction type
     */
    public void setTransactionType(TsurugiJdbcTransactionType type) {
        config.setTransactionType(type);
    }

    /**
     * Get transaction type.
     *
     * @return transaction type
     */
    public TsurugiJdbcTransactionType getTransactionType() {
        return config.getTransactionType();
    }

    /**
     * Set transaction label.
     *
     * @param transactionLabel transaction label
     */
    public void setTransactionLabel(String transactionLabel) {
        config.setTransactionLabel(transactionLabel);
    }

    /**
     * Get transaction label.
     *
     * @return transaction label
     */
    public String getTransactionLabel() {
        return config.getTransactionLabel();
    }

    /**
     * Set LTX include DDL.
     *
     * @param include include DDL
     */
    public void setTransactionIncludeDdl(boolean include) {
        config.setIncludeDdl(include);
    }

    /**
     * Get LTX include DDL.
     *
     * @return true if include DDL, false otherwise
     */
    public boolean getTransactionIncludeDdl() {
        return config.getIncludeDdl();
    }

    /**
     * Set LTX write preserve.
     *
     * @param tableNames table names
     */
    public void setWritePreserve(List<String> tableNames) {
        config.setWritePreserve(tableNames);
    }

    /**
     * Get LTX write preserve.
     *
     * @return table names
     */
    public @Nullable List<String> getWritePreserve() {
        return config.getWritePreserve();
    }

    /**
     * Set LTX inclusive read area.
     *
     * @param tableNames table names
     */
    public void setInclusiveReadArea(List<String> tableNames) {
        config.setInclusiveReadArea(tableNames);
    }

    /**
     * Get LTX inclusive read area.
     *
     * @return table names
     */
    public @Nullable List<String> getInclusiveReadArea() {
        return config.getInclusiveReadArea();
    }

    /**
     * Set LTX exclusive read area.
     *
     * @param tableNames table names
     */
    public void setExclusiveReadArea(List<String> tableNames) {
        config.setExclusiveReadArea(tableNames);
    }

    /**
     * Get LTX exclusive read area.
     *
     * @return table names
     */
    public @Nullable List<String> getExclusiveReadArea() {
        return config.getExclusiveReadArea();
    }

    /**
     * Set RTX scan parallel.
     *
     * @param scanParallel scan parallel
     */
    public void setTransactionScanParallel(int scanParallel) {
        config.setScanParallel(scanParallel);
    }

    /**
     * Get RTX scan parallel.
     *
     * @return scan parallel
     */
    public OptionalInt getTransactionScanParallel() {
        return config.getScanParallel();
    }

    /**
     * Get or create transaction.
     *
     * @return transaction
     * @throws SQLException if a database access error occurs
     */
    @TsurugiJdbcInternal
    public synchronized TsurugiJdbcTransaction getTransaction() throws SQLException {
        var transaction = getFieldTransaction();
        if (transaction != null) {
            return transaction;
        }

        var option = getLowTransactionOption();
        LOG.config(() -> String.format("transactionOption=%s", option));

        int timeout = config.getBeginTimeout();
        LOG.config(() -> String.format("beginTimeout=%d [seconds]", timeout));

        Transaction lowTransaction;
        try {
            lowTransaction = lowSqlClient.createTransaction(option).await(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("Transaction create error", e);
        }

        transaction = factory.createTransaction(lowTransaction, getAutoCommit(), config);
        this.transaction = transaction;
        return transaction;
    }

    /**
     * Get transaction option.
     *
     * @return transaction option
     */
    protected SqlRequest.TransactionOption getLowTransactionOption() {
        return config.getLowTransactionOption();
    }

    /**
     * Check transaction active.
     *
     * @return transaction
     * @throws SQLException if transaction is not active
     */
    protected TsurugiJdbcTransaction checkTransactionActive() throws SQLException {
        var transaction = getFieldTransaction();
        if (transaction == null) {
            throw getExceptionHandler().transactionNotFoundException();
        }
        return transaction;
    }

    /**
     * Check transaction inactive.
     *
     * @throws SQLException if transaction is active
     */
    protected void checkTransactionInactive() throws SQLException {
        var transaction = getFieldTransaction();
        if (transaction != null) {
            throw getExceptionHandler().transactionFoundException();
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
        config.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return config.getAutoCommit();
    }

    /**
     * Set commit type.
     *
     * @param type commit type
     */
    public void setCommitType(TsurugiJdbcCommitType type) {
        config.setCommitType(type);
    }

    /**
     * Get commit type.
     *
     * @return commit type
     */
    public TsurugiJdbcCommitType getCommitType() {
        return config.getCommitType();
    }

    /**
     * Set automatically dispose upon commit.
     *
     * @param autoDispose automatically dispose
     */
    public void setCommitAutoDispose(boolean autoDispose) {
        config.setAutoDispose(autoDispose);
    }

    /**
     * Get automatically dispose upon commit.
     *
     * @return true if automatically dispose, false otherwise
     */
    public boolean getCommitAutoDispose() {
        return config.getAutoDispose();
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
    public TsurugiJdbcDatabaseMetaData getMetaData() throws SQLException {
        if (this.metaData == null) {
            this.metaData = new TsurugiJdbcDatabaseMetaData(this);
        }
        return this.metaData;
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
        return null; // FIXME getWarnings()
    }

    @Override
    public void clearWarnings() throws SQLException {
        // do nothing
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
            config.put(name, value, failedProperties);
        } catch (Exception e) {
            throw getExceptionHandler().clientInfoException(e, failedProperties);
        }

        if (!failedProperties.isEmpty()) {
            throw getExceptionHandler().clientInfoException(null, failedProperties);
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
                this.config.put(key, value, failedProperties);
            } catch (Exception e) {
                exceptionList.add(e);
            }
        }

        if (!failedProperties.isEmpty()) {
            var firstException = !exceptionList.isEmpty() ? exceptionList.get(0) : null;
            var e = getExceptionHandler().clientInfoException(firstException, failedProperties);
            for (int i = 1; i < exceptionList.size(); i++) {
                e.addSuppressed(exceptionList.get(i));
            }
            throw e;
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        var property = config.getInternalProperties().getProperty(name);
        if (property == null) {
            return null;
        }
        return property.getStringValue();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        var properties = config.getInternalProperties().getProperties();
        var result = new Properties(properties.size());
        for (var property : properties) {
            String key = property.name();
            String value = property.getStringValue();
            if (value == null) {
                value = "";
            }
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
    @TsurugiJdbcNotSupported
    public void abort(Executor executor) throws SQLException {
        // FIXME TsurugiJdbcConnection.abort(): shutdown(FORCEFUL)を実行するか？
        throw new SQLFeatureNotSupportedException("abort not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNetworkTimeout not supported");
    }

    @Override
    @TsurugiJdbcNotSupported
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("getNetworkTimeout not supported");
    }

    @Override
    public void close() throws SQLException {
        LowCloser shutdown;
        {
            var shutdownType = config.getShutdownType();
            LOG.config(() -> String.format("shutdownType=%s", shutdownType));

            var lowShutdownType = shutdownType.getLowShutdownType();
            if (lowShutdownType != null) {
                int timeout = config.getShutdownTimeout();
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
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("Connection close error", e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return lowSession.isClosed();
    }
}
