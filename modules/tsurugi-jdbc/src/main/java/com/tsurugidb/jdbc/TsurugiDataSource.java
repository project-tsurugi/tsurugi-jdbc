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

public class TsurugiDataSource implements DataSource, HasFactory {
    private static final Logger PARENT_LOGGER = Logger.getLogger(TsurugiDataSource.class.getPackageName());

    private TsurugiJdbcFactory factory = TsurugiJdbcFactory.getDefaultFactory();
    private final TsurugiConfig config;

    public TsurugiDataSource() {
        this(new TsurugiConfig());
    }

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

    public TsurugiConfig getConfig() {
        return this.config;
    }

    public void setJdbcUrl(String url) throws SQLException {
        this.config.setJdbcUrl(getFactory(), url);
    }

    // Session

    public void setEndpoint(String endpoint) {
        config.setEndpoint(endpoint);
    }

    public void setUser(String user) throws SQLException {
        config.setUser(user);
    }

    public void setPassword(String password) throws SQLException {
        config.setPassword(password);
    }

    public void setAuthToken(String authToken) throws SQLException {
        config.setAuthToken(authToken);
    }

    public void setCredentials(String path) throws SQLException {
        config.setCredentials(path);
    }

    public void setApplicationName(String applicationName) {
        config.setApplicationName(applicationName);
    }

    public void setSessionLabel(String sessionLabel) {
        config.setSessionLabel(sessionLabel);
    }

    public void setKeepAlive(boolean keepAlive) {
        config.setKeepAlive(keepAlive);
    }

    public void setConnectTimeout(int seconds) {
        config.setConnectTimeout(seconds);
    }

    public void setShutdownType(TsurugiJdbcShutdownType shutdownType) {
        config.setShutdownType(shutdownType);
    }

    public void setShutdownTimeout(int seconds) {
        config.setShutdownTimeout(seconds);
    }

    // Transaction

    public void setTransactionType(TsurugiJdbcTransactionType transactionType) {
        config.setTransactionType(transactionType);
    }

    public void setTransactionLabel(String transactionLabel) {
        config.setTransactionLabel(transactionLabel);
    }

    public void setTransactionIncludeDdl(boolean includeDdl) {
        config.setTransactionIncludeDdl(includeDdl);
    }

    public void setWritePreserve(List<String> tableNames) {
        config.setWritePreserve(tableNames);
    }

    public void setInclusiveReadArea(List<String> tableNames) {
        config.setInclusiveReadArea(tableNames);
    }

    public void setExclusiveReadArea(List<String> tableNames) {
        config.setExclusiveReadArea(tableNames);
    }

    public void setTransactionScanParallel(int scanParallel) {
        config.setTransactionScanParallel(scanParallel);
    }

    public void setAutoCommit(boolean autoCommit) {
        config.setAutoCommit(autoCommit);
    }

    public void setCommitType(TsurugiJdbcCommitType commitType) {
        config.setCommitType(commitType);
    }

    public void setCommitAutoDispose(boolean autoDispose) {
        config.setCommitAutoDispose(autoDispose);
    }

    public void setBeginTimeout(int seconds) {
        config.setBeginTimeout(seconds);
    }

    public void setCommitTimeout(int seconds) {
        config.setCommitTimeout(seconds);
    }

    public void setRollbackTimeout(int seconds) {
        config.setRollbackTimeout(seconds);
    }

    // Statement

    public void setExecuteTimeout(int seconds) {
        config.setExecuteTimeout(seconds);
    }

    // ResultSet

    public void setQueryTimeout(int seconds) {
        config.setQueryTimeout(seconds);
    }

    // Common

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
