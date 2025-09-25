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

import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKey;
import java.util.List;
import java.util.Objects;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;

/**
 * Tsurugi JDBC Connection Builder.
 */
public class TsurugiJdbcConnectionBuilder implements ConnectionBuilder {

    private final TsurugiConfig config = new TsurugiConfig();

    // Session

    /**
     * Set endpoint.
     *
     * @param endpoint endpoint
     * @return this
     */
    public TsurugiJdbcConnectionBuilder endpoint(String endpoint) {
        config.setEndpoint(endpoint);
        return this;
    }

    @Override
    public TsurugiJdbcConnectionBuilder user(String username) {
        config.setUser(username);
        return this;
    }

    @Override
    public TsurugiJdbcConnectionBuilder password(String password) {
        config.setPassword(password);
        return this;
    }

    /**
     * Set authentication token.
     *
     * @param authToken authentication token
     * @return this
     */
    public TsurugiJdbcConnectionBuilder authToken(String authToken) {
        config.setAuthToken(authToken);
        return this;
    }

    /**
     * Set credentials.
     *
     * @param path credential file path
     * @return this
     */
    public TsurugiJdbcConnectionBuilder credentials(String path) {
        config.setCredentials(path);
        return this;
    }

    /**
     * Set application name.
     *
     * @param applicationName application name
     * @return this
     */
    public TsurugiJdbcConnectionBuilder applicationName(String applicationName) {
        config.setApplicationName(applicationName);
        return this;
    }

    /**
     * Set session label.
     *
     * @param sessionLabel session label
     * @return this
     */
    public TsurugiJdbcConnectionBuilder sessionLabel(String sessionLabel) {
        config.setSessionLabel(sessionLabel);
        return this;
    }

    /**
     * Set session keep alive.
     *
     * @param keepAlive keep alive
     * @return this
     */
    public TsurugiJdbcConnectionBuilder keepAlive(boolean keepAlive) {
        config.setKeepAlive(keepAlive);
        return this;
    }

    /**
     * Set connect timeout.
     *
     * @param seconds connect timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder connectTimeout(int seconds) {
        config.setConnectTimeout(seconds);
        return this;
    }

    /**
     * Set session shutdown type.
     *
     * @param shutdownType shutdown type
     * @return this
     */
    public TsurugiJdbcConnectionBuilder shutdownType(TsurugiJdbcShutdownType shutdownType) {
        config.setShutdownType(shutdownType);
        return this;
    }

    /**
     * Set session shutdown timeout.
     *
     * @param seconds shutdown timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder shutdownTimeout(int seconds) {
        config.setShutdownTimeout(seconds);
        return this;
    }

    // Transaction

    /**
     * Set transaction type.
     *
     * @param transactionType transaction type
     * @return this
     */
    public TsurugiJdbcConnectionBuilder transactionType(TsurugiJdbcTransactionType transactionType) {
        config.setTransactionType(transactionType);
        return this;
    }

    /**
     * Set transaction label.
     *
     * @param transactionLabel transaction label
     * @return this
     */
    public TsurugiJdbcConnectionBuilder transactionLabel(String transactionLabel) {
        config.setTransactionLabel(transactionLabel);
        return this;
    }

    /**
     * Set LTX include DDL.
     *
     * @param includeDdl include DDL
     * @return this
     */
    public TsurugiJdbcConnectionBuilder transactionIncludeDdl(boolean includeDdl) {
        config.setTransactionIncludeDdl(includeDdl);
        return this;
    }

    /**
     * Set LTX write preserve.
     *
     * @param tableNames table names
     * @return this
     */
    public TsurugiJdbcConnectionBuilder writePreserve(List<String> tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setWritePreserve(tableNames);
        return this;
    }

    /**
     * Set LTX write preserve.
     *
     * @param tableNames table names
     * @return this
     */
    public TsurugiJdbcConnectionBuilder writePreserve(String... tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setWritePreserve(List.of(tableNames));
        return this;
    }

    /**
     * Set LTX inclusive read area.
     *
     * @param tableNames table names
     * @return this
     */
    public TsurugiJdbcConnectionBuilder inclusiveReadArea(List<String> tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setInclusiveReadArea(tableNames);
        return this;
    }

    /**
     * Set LTX inclusive read area.
     *
     * @param tableNames table names
     * @return this
     */
    public TsurugiJdbcConnectionBuilder inclusiveReadArea(String... tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setInclusiveReadArea(List.of(tableNames));
        return this;
    }

    /**
     * Set LTX exclusive read area.
     *
     * @param tableNames table names
     * @return this
     */
    public TsurugiJdbcConnectionBuilder exclusiveReadArea(List<String> tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setExclusiveReadArea(tableNames);
        return this;
    }

    /**
     * Set LTX exclusive read area.
     *
     * @param tableNames table names
     * @return this
     */
    public TsurugiJdbcConnectionBuilder exclusiveReadArea(String... tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setExclusiveReadArea(List.of(tableNames));
        return this;
    }

    /**
     * Set RTX scan parallel.
     *
     * @param scanParallel scan parallel
     * @return this
     */
    public TsurugiJdbcConnectionBuilder transactionScanParallel(int scanParallel) {
        config.setTransactionScanParallel(scanParallel);
        return this;
    }

    /**
     * Set auto commit.
     *
     * @param autoCommit auto commit
     * @return this
     */
    public TsurugiJdbcConnectionBuilder autoCommit(boolean autoCommit) {
        config.setAutoCommit(autoCommit);
        return this;
    }

    /**
     * Set commit type.
     *
     * @param commitType commit type
     * @return this
     */
    public TsurugiJdbcConnectionBuilder commitType(TsurugiJdbcCommitType commitType) {
        config.setCommitType(commitType);
        return this;
    }

    /**
     * Set automatically dispose upon commit.
     *
     * @param autoDispose automatically dispose
     * @return this
     */
    public TsurugiJdbcConnectionBuilder commitAutoDispose(boolean autoDispose) {
        config.setCommitAutoDispose(autoDispose);
        return this;
    }

    /**
     * Set transaction begin timeout.
     *
     * @param seconds begin timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder beginTimeout(int seconds) {
        config.setBeginTimeout(seconds);
        return this;
    }

    /**
     * Set transaction commit timeout.
     *
     * @param seconds commit timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder commitTimeout(int seconds) {
        config.setCommitTimeout(seconds);
        return this;
    }

    /**
     * Set transaction rollback timeout.
     *
     * @param seconds rollback timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder rollbackTimeout(int seconds) {
        config.setRollbackTimeout(seconds);
        return this;
    }

    // Statement

    /**
     * Set statement execute timeout.
     *
     * @param seconds execute timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder executeTimeout(int seconds) {
        config.setExecuteTimeout(seconds);
        return this;
    }

    // ResultSet

    /**
     * Set SELECT timeout.
     *
     * @param seconds SELECT timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder queryTimeout(int seconds) {
        config.setQueryTimeout(seconds);
        return this;
    }

    // Common

    /**
     * Set default timeout.
     *
     * @param seconds default timeout [seconds]
     * @return this
     */
    public TsurugiJdbcConnectionBuilder defaultTimeout(int seconds) {
        config.setDefaultTimeout(seconds);
        return this;
    }

    // other

    @Override
    @TsurugiJdbcNotSupported
    public TsurugiJdbcConnectionBuilder shardingKey(ShardingKey shardingKey) {
        // not supported
        return this;
    }

    @Override
    @TsurugiJdbcNotSupported
    public TsurugiJdbcConnectionBuilder superShardingKey(ShardingKey superShardingKey) {
        // not supported
        return this;
    }

    // build

    @Override
    public TsurugiJdbcConnection build() throws SQLException {
        return TsurugiDriver.getTsurugiDriver().connect(this.config);
    }
}
