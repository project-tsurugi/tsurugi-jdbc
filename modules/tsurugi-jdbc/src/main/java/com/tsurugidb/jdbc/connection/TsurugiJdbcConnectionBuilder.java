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

public class TsurugiJdbcConnectionBuilder implements ConnectionBuilder {

    private final TsurugiConfig config = new TsurugiConfig();

    // Session

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

    public TsurugiJdbcConnectionBuilder authToken(String authToken) {
        config.setAuthToken(authToken);
        return this;
    }

    public TsurugiJdbcConnectionBuilder credentials(String path) {
        config.setCredentials(path);
        return this;
    }

    public TsurugiJdbcConnectionBuilder applicationName(String applicationName) {
        config.setApplicationName(applicationName);
        return this;
    }

    public TsurugiJdbcConnectionBuilder sessionLabel(String sessionLabel) {
        config.setSessionLabel(sessionLabel);
        return this;
    }

    public TsurugiJdbcConnectionBuilder keepAlive(boolean keepAlive) {
        config.setKeepAlive(keepAlive);
        return this;
    }

    public TsurugiJdbcConnectionBuilder connectTimeout(int seconds) {
        config.setConnectTimeout(seconds);
        return this;
    }

    public TsurugiJdbcConnectionBuilder shutdownType(TsurugiJdbcShutdownType shutdownType) {
        config.setShutdownType(shutdownType);
        return this;
    }

    public TsurugiJdbcConnectionBuilder shutdownTimeout(int seconds) {
        config.setShutdownTimeout(seconds);
        return this;
    }

    // Transaction

    public TsurugiJdbcConnectionBuilder transactionType(TsurugiJdbcTransactionType transactionType) {
        config.setTransactionType(transactionType);
        return this;
    }

    public TsurugiJdbcConnectionBuilder transactionLabel(String transactionLabel) {
        config.setTransactionLabel(transactionLabel);
        return this;
    }

    public TsurugiJdbcConnectionBuilder transactionIncludeDdl(boolean includeDdl) {
        config.setTransactionIncludeDdl(includeDdl);
        return this;
    }

    public TsurugiJdbcConnectionBuilder writePreserve(List<String> tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setWritePreserve(tableNames);
        return this;
    }

    public TsurugiJdbcConnectionBuilder writePreserve(String... tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setWritePreserve(List.of(tableNames));
        return this;
    }

    public TsurugiJdbcConnectionBuilder inclusiveReadArea(List<String> tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setInclusiveReadArea(tableNames);
        return this;
    }

    public TsurugiJdbcConnectionBuilder inclusiveReadArea(String... tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setInclusiveReadArea(List.of(tableNames));
        return this;
    }

    public TsurugiJdbcConnectionBuilder exclusiveReadArea(List<String> tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setExclusiveReadArea(tableNames);
        return this;
    }

    public TsurugiJdbcConnectionBuilder exclusiveReadArea(String... tableNames) {
        Objects.requireNonNull(tableNames, "tableNames is null");
        config.setExclusiveReadArea(List.of(tableNames));
        return this;
    }

    public TsurugiJdbcConnectionBuilder transactionScanParallel(int scanParallel) {
        config.setTransactionScanParallel(scanParallel);
        return this;
    }

    public TsurugiJdbcConnectionBuilder autoCommit(boolean autoCommit) {
        config.setAutoCommit(autoCommit);
        return this;
    }

    public TsurugiJdbcConnectionBuilder commitType(TsurugiJdbcCommitType commitType) {
        config.setCommitType(commitType);
        return this;
    }

    public TsurugiJdbcConnectionBuilder commitAutoDispose(boolean autoDispose) {
        config.setCommitAutoDispose(autoDispose);
        return this;
    }

    public TsurugiJdbcConnectionBuilder beginTimeout(int seconds) {
        config.setBeginTimeout(seconds);
        return this;
    }

    public TsurugiJdbcConnectionBuilder commitTimeout(int seconds) {
        config.setCommitTimeout(seconds);
        return this;
    }

    public TsurugiJdbcConnectionBuilder rollbackTimeout(int seconds) {
        config.setRollbackTimeout(seconds);
        return this;
    }

    // Statement

    public TsurugiJdbcConnectionBuilder executeTimeout(int seconds) {
        config.setExecuteTimeout(seconds);
        return this;
    }

    // ResultSet

    public TsurugiJdbcConnectionBuilder queryTimeout(int seconds) {
        config.setQueryTimeout(seconds);
        return this;
    }

    // Common

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
