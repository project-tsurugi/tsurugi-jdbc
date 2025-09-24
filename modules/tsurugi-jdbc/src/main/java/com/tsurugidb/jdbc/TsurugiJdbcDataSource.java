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
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.connection.TsurugiJdbcShutdownType;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.HasFactory;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;

public class TsurugiJdbcDataSource implements DataSource, HasFactory {
    private static final Logger PARENT_LOGGER = Logger.getLogger(TsurugiJdbcDataSource.class.getPackageName());

    private TsurugiJdbcFactory factory = TsurugiJdbcFactory.getDefaultFactory();
    private final TsurugiJdbcProperties properties;

    public TsurugiJdbcDataSource() {
        this(new TsurugiJdbcProperties());
    }

    public TsurugiJdbcDataSource(TsurugiJdbcProperties properties) {
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

    public TsurugiJdbcProperties getProperties() {
        return properties;
    }

    public void setJdbcUrl(String url) throws SQLException {
        this.properties.setJdbcUrl(getFactory(), url);
    }

    // Session

    public void setEndpoint(String endpoint) {
        properties.setEndpoint(endpoint);
    }

    public void setUser(String user) throws SQLException {
        properties.setUser(user);
    }

    public void setPassword(String password) throws SQLException {
        properties.setPassword(password);
    }

    public void setAuthToken(String authToken) throws SQLException {
        properties.setAuthToken(authToken);
    }

    public void setCredentials(String path) throws SQLException {
        properties.setCredentials(path);
    }

    public void setApplicationName(String applicationName) {
        properties.setApplicationName(applicationName);
    }

    public void setSessionLabel(String sessionLabel) {
        properties.setSessionLabel(sessionLabel);
    }

    public void setKeepAlive(boolean keepAlive) {
        properties.setKeepAlive(keepAlive);
    }

    public void setConnectTimeout(int seconds) {
        properties.setConnectTimeout(seconds);
    }

    public void setShutdownType(TsurugiJdbcShutdownType shutdownType) {
        properties.setShutdownType(shutdownType);
    }

    public void setShutdownTimeout(int seconds) {
        properties.setShutdownTimeout(seconds);
    }

    // Transaction

    public void setTransactionType(TsurugiJdbcTransactionType transactionType) {
        properties.setTransactionType(transactionType);
    }

    public void setTransactionLabel(String transactionLabel) {
        properties.setTransactionLabel(transactionLabel);
    }

    public void setTransactionIncludeDdl(boolean includeDdl) {
        properties.setTransactionIncludeDdl(includeDdl);
    }

    public void setWritePreserve(List<String> tableNames) {
        properties.setWritePreserve(tableNames);
    }

    public void setInclusiveReadArea(List<String> tableNames) {
        properties.setInclusiveReadArea(tableNames);
    }

    public void setExclusiveReadArea(List<String> tableNames) {
        properties.setExclusiveReadArea(tableNames);
    }

    public void setTransactionScanParallel(int scanParallel) {
        properties.setTransactionScanParallel(scanParallel);
    }

    public void setAutoCommit(boolean autoCommit) {
        properties.setAutoCommit(autoCommit);
    }

    public void setCommitType(TsurugiJdbcCommitType commitType) {
        properties.setCommitType(commitType);
    }

    public void setCommitAutoDispose(boolean autoDispose) {
        properties.setCommitAutoDispose(autoDispose);
    }

    public void setBeginTimeout(int seconds) {
        properties.setBeginTimeout(seconds);
    }

    public void setCommitTimeout(int seconds) {
        properties.setCommitTimeout(seconds);
    }

    public void setRollbackTimeout(int seconds) {
        properties.setRollbackTimeout(seconds);
    }

    // Statement

    public void setExecuteTimeout(int seconds) {
        properties.setExecuteTimeout(seconds);
    }

    // ResultSet

    public void setQueryTimeout(int seconds) {
        properties.setQueryTimeout(seconds);
    }

    // Common

    public void setDefaultTimeout(int seconds) {
        properties.setDefaultTimeout(seconds);
    }

    // connect

    @Override
    public Connection getConnection() throws SQLException {
        return TsurugiDriver.getTsurugiDriver().connect(this.properties);
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
        properties.setConnectTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return properties.getConnectTimeout();
    }

    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
        // TODO Auto-generated method stub: createConnectionBuilder()
        return null;
    }
}
