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
package com.tsurugidb.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionBuilder;
import com.tsurugidb.jdbc.connection.TsurugiJdbcShutdownType;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;

/**
 * Tsurugi JDBC DataSource.
 */
public class TsurugiDataSource implements DataSource, HasFactory {
    private static final Logger PARENT_LOGGER = Logger.getLogger(TsurugiDataSource.class.getPackageName());

    private TsurugiJdbcFactory factory = TsurugiJdbcFactory.getDefaultFactory();
    private final TsurugiConfig config;

    /**
     * Creates a new instance.
     */
    public TsurugiDataSource() {
        this(new TsurugiConfig());
    }

    /**
     * Creates a new instance.
     *
     * @param config configuration
     */
    public TsurugiDataSource(TsurugiConfig config) {
        this.config = config;
    }

    @Override
    public void setFactory(TsurugiJdbcFactory factory) {
        this.factory = factory;
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

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return PARENT_LOGGER;
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

    /**
     * Get configuration.
     *
     * @return configuration
     */
    public TsurugiConfig getConfig() {
        return this.config;
    }

    /**
     * Set JDBC URL.
     *
     * @param url JDBC URL
     * @throws SQLException if the url is null
     */
    public void setJdbcUrl(String url) throws SQLException {
        this.config.setJdbcUrl(getFactory(), url);
    }

    // Session

    /**
     * Set endpoint.
     *
     * @param endpoint endpoint
     */
    public void setEndpoint(String endpoint) {
        config.setEndpoint(endpoint);
    }

    /**
     * Set user.
     *
     * @param user user name
     */
    public void setUser(String user) {
        config.setUser(user);
    }

    /**
     * Set password.
     *
     * @param password password
     */
    public void setPassword(String password) {
        config.setPassword(password);
    }

    /**
     * Set authentication token.
     *
     * @param authToken authentication token
     */
    public void setAuthToken(String authToken) {
        config.setAuthToken(authToken);
    }

    /**
     * Set credentials.
     *
     * @param path credential file path
     */
    public void setCredentials(String path) {
        config.setCredentials(path);
    }

    /**
     * Set application name.
     *
     * @param applicationName application name
     */
    public void setApplicationName(String applicationName) {
        config.setApplicationName(applicationName);
    }

    /**
     * Set session label.
     *
     * @param sessionLabel session label
     */
    public void setSessionLabel(String sessionLabel) {
        config.setSessionLabel(sessionLabel);
    }

    /**
     * Set session keep alive.
     *
     * @param keepAlive keep alive
     */
    public void setKeepAlive(boolean keepAlive) {
        config.setKeepAlive(keepAlive);
    }

    /**
     * Set connect timeout.
     *
     * @param seconds connect timeout [seconds]
     */
    public void setConnectTimeout(int seconds) {
        config.setConnectTimeout(seconds);
    }

    /**
     * Set session shutdown type.
     *
     * @param shutdownType shutdown type
     */
    public void setShutdownType(TsurugiJdbcShutdownType shutdownType) {
        config.setShutdownType(shutdownType);
    }

    /**
     * Set session shutdown timeout.
     *
     * @param seconds shutdown timeout [seconds]
     */
    public void setShutdownTimeout(int seconds) {
        config.setShutdownTimeout(seconds);
    }

    // Transaction

    /**
     * Set transaction type.
     *
     * @param transactionType transaction type
     */
    public void setTransactionType(TsurugiJdbcTransactionType transactionType) {
        config.setTransactionType(transactionType);
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
     * Set transaction include DDL.
     *
     * @param includeDdl include DDL
     */
    public void setTransactionIncludeDdl(boolean includeDdl) {
        config.setTransactionIncludeDdl(includeDdl);
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
     * Set LTX inclusive read area.
     *
     * @param tableNames table names
     */
    public void setInclusiveReadArea(List<String> tableNames) {
        config.setInclusiveReadArea(tableNames);
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
     * Set RTX scan parallel.
     *
     * @param scanParallel scan parallel
     */
    public void setTransactionScanParallel(int scanParallel) {
        config.setTransactionScanParallel(scanParallel);
    }

    /**
     * Set auto commit.
     *
     * @param autoCommit auto commit
     */
    public void setAutoCommit(boolean autoCommit) {
        config.setAutoCommit(autoCommit);
    }

    /**
     * Set commit type.
     *
     * @param commitType commit type
     */
    public void setCommitType(TsurugiJdbcCommitType commitType) {
        config.setCommitType(commitType);
    }

    /**
     * Set automatically dispose upon commit.
     *
     * @param autoDispose automatically dispose
     */
    public void setCommitAutoDispose(boolean autoDispose) {
        config.setCommitAutoDispose(autoDispose);
    }

    /**
     * Set transaction begin timeout.
     *
     * @param seconds begin timeout [seconds]
     */
    public void setBeginTimeout(int seconds) {
        config.setBeginTimeout(seconds);
    }

    /**
     * Set transaction commit timeout.
     *
     * @param seconds commit timeout [seconds]
     */
    public void setCommitTimeout(int seconds) {
        config.setCommitTimeout(seconds);
    }

    /**
     * Set transaction rollback timeout.
     *
     * @param seconds rollback timeout [seconds]
     */
    public void setRollbackTimeout(int seconds) {
        config.setRollbackTimeout(seconds);
    }

    // Statement

    /**
     * Set execute timeout.
     *
     * @param seconds execute timeout [seconds]
     */
    public void setExecuteTimeout(int seconds) {
        config.setExecuteTimeout(seconds);
    }

    // ResultSet

    /**
     * Set SELECT timeout.
     *
     * @param seconds SELECT timeout [seconds]
     */
    public void setQueryTimeout(int seconds) {
        config.setQueryTimeout(seconds);
    }

    // Common

    /**
     * Set default timeout.
     *
     * @param seconds default timeout [seconds]
     */
    public void setDefaultTimeout(int seconds) {
        config.setDefaultTimeout(seconds);
    }

    // connect

    @Override
    public TsurugiJdbcConnection getConnection() throws SQLException {
        return TsurugiDriver.getTsurugiDriver().connect(this.config);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        setUser(username);
        setPassword(password);
        return getConnection();
    }

    @Override
    @TsurugiJdbcNotSupported
    public PrintWriter getLogWriter() throws SQLException {
        return null; // not supported
    }

    @Override
    @TsurugiJdbcNotSupported
    public void setLogWriter(PrintWriter out) throws SQLException {
        // not supported
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is the same as {@link #setConnectTimeout(int)}.
     * </p>
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        config.setConnectTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return config.getConnectTimeout();
    }

    @Override
    public TsurugiJdbcConnectionBuilder createConnectionBuilder() throws SQLException {
        return new TsurugiJdbcConnectionBuilder();
    }
}
