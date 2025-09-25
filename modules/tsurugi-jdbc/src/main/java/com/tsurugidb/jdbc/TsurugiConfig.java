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

import java.io.IOException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.OptionalInt;
import java.util.Properties;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.connection.TsurugiJdbcShutdownType;
import com.tsurugidb.jdbc.driver.TsurugiJdbcUrlParser;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.property.TsurugiJdbcProperties;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyBoolean;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyEnum;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyInt;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyString;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyStringList;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * Tsurugi JDBC Configuration.
 */
public class TsurugiConfig {
    // Session
    /** user */
    public static final String USER = "user";
    /** password */
    public static final String PASSWORD = "password";
    /** authentication token */
    public static final String AUTH_TOKEN = "authToken";
    /** credential file path */
    public static final String CREDENTIALS = "credentials";
    /** application name */
    public static final String APPLICATION_NAME = "applicationName";
    /** session label */
    public static final String SESSION_LABEL = "sessionLabel";
    /** session keep alive */
    public static final String KEEP_ALIVE = "keepAlive";
    /** session connect timeout [seconds] */
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    /** session shutdown type */
    public static final String SHUTDOWN_TYPE = "shutdownType";
    /** session shutdown timeout [seconds] */
    public static final String SHUTDOWN_TIMEOUT = "shutdownTimeout";

    // Transaction
    /** transaction type */
    public static final String TRANSACTION_TYPE = "transactionType";
    /** transaction label */
    public static final String TRANSACTION_LABEL = "transactionLabel";
    /** LTX include DDL */
    public static final String INCLUDE_DDL = "includeDdl";
    /** LTX write preserve */
    public static final String WRITE_PRESERVE = "writePreserve";
    /** LTX inclusive read area */
    public static final String INCLUSIVE_READ_AREA = "inclusiveReadArea";
    /** LTX exclusive read area */
    public static final String EXCLUSIVE_READ_AREA = "exclusiveReadArea";
    /** RTX scan parallel */
    public static final String SCAN_PARALLEL = "scanParallel";
    /** auto commit */
    public static final String AUTO_COMMIT = "autoCommit";
    /** commit type */
    public static final String COMMIT_TYPE = "commitType";
    /** automatically dispose upon commit */
    public static final String AUTO_DISPOSE = "autoDispose";
    /** transaction begin timeout [seconds] */
    public static final String BEGIN_TIMEOUT = "beginTimeout";
    /** transaction commit timeout [seconds] */
    public static final String COMMIT_TIMEOUT = "commitTimeout";
    /** transaction rollback timeout [seconds] */
    public static final String ROLLBACK_TIMEOUT = "rollbackTimeout";

    // Statement
    /** transaction execute timeout [seconds] */
    public static final String EXECUTE_TIMEOUT = "executeTimeout";

    // ResultSet
    /** SELECT timeout [seconds] */
    public static final String QUERY_TIMEOUT = "queryTimeout";

    // Common
    /** default timeout [seconds] */
    public static final String DEFAULT_TIMEOUT = "defaultTimeout";

    private String endpoint;

    private final TsurugiJdbcPropertyString user = new TsurugiJdbcPropertyString(USER).description("user");
    private final TsurugiJdbcPropertyString password = new TsurugiJdbcPropertyString(PASSWORD).description("password");
    private final TsurugiJdbcPropertyString authToken = new TsurugiJdbcPropertyString(AUTH_TOKEN).description("authentication token");
    private final TsurugiJdbcPropertyString credentials = new TsurugiJdbcPropertyString(CREDENTIALS).description("credential file path");
    private final TsurugiJdbcPropertyString applicationName = new TsurugiJdbcPropertyString(APPLICATION_NAME).description("application name");
    private final TsurugiJdbcPropertyString sessionLabel = new TsurugiJdbcPropertyString(SESSION_LABEL).description("session label");
    private final TsurugiJdbcPropertyBoolean keepAlive = new TsurugiJdbcPropertyBoolean(KEEP_ALIVE).defaultValue(true).description("session keep alive");
    private final TsurugiJdbcPropertyInt connectTimeout = new TsurugiJdbcPropertyInt(CONNECT_TIMEOUT).description("connect timeout [seconds]").defaultValue(() -> DriverManager.getLoginTimeout());
    private final TsurugiJdbcPropertyEnum<TsurugiJdbcShutdownType> shutdownType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcShutdownType.class, SHUTDOWN_TYPE)
            .defaultValue(TsurugiJdbcShutdownType.GRACEFUL).description("session shutdown type");
    private final TsurugiJdbcPropertyInt shutdownTimeout = new TsurugiJdbcPropertyInt(SHUTDOWN_TIMEOUT).description("session shutdown timeout [seconds]");

    private final TsurugiJdbcPropertyEnum<TsurugiJdbcTransactionType> transactionType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcTransactionType.class, TRANSACTION_TYPE)
            .defaultValue(TsurugiJdbcTransactionType.OCC).description("transaction type");
    private final TsurugiJdbcPropertyString transactionLabel = new TsurugiJdbcPropertyString(TRANSACTION_LABEL).description("transaction label");
    private final TsurugiJdbcPropertyBoolean includeDdl = new TsurugiJdbcPropertyBoolean(INCLUDE_DDL).defaultValue(false).description("LTX include DDL");
    private final TsurugiJdbcPropertyStringList writePreserve = new TsurugiJdbcPropertyStringList(WRITE_PRESERVE).description("LTX write preserve table names (comma separate)");
    private final TsurugiJdbcPropertyStringList inclusiveReadArea = new TsurugiJdbcPropertyStringList(INCLUSIVE_READ_AREA).description("LTX inclusive read area table names (comma separate)");
    private final TsurugiJdbcPropertyStringList exclusiveReadArea = new TsurugiJdbcPropertyStringList(EXCLUSIVE_READ_AREA).description("LTX exclusive read area table names (comma separate)");
    private final TsurugiJdbcPropertyInt scanParallel = new TsurugiJdbcPropertyInt(SCAN_PARALLEL).description("RTX scan parallel");
    private final TsurugiJdbcPropertyBoolean autoCommit = new TsurugiJdbcPropertyBoolean(AUTO_COMMIT).defaultValue(true).description("auto commit");
    private final TsurugiJdbcPropertyEnum<TsurugiJdbcCommitType> commitType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcCommitType.class, COMMIT_TYPE).defaultValue(TsurugiJdbcCommitType.DEFAULT)
            .description("commit type");
    private final TsurugiJdbcPropertyBoolean autoDispose = new TsurugiJdbcPropertyBoolean(AUTO_DISPOSE).defaultValue(false).description("automatically dispose upon commit");
    private final TsurugiJdbcPropertyInt beginTimeout = new TsurugiJdbcPropertyInt(BEGIN_TIMEOUT).description("transaction begin timeout [seconds]");
    private final TsurugiJdbcPropertyInt commitTimeout = new TsurugiJdbcPropertyInt(COMMIT_TIMEOUT).description("transaction commit timeout [seconds]");
    private final TsurugiJdbcPropertyInt rollbackTimeout = new TsurugiJdbcPropertyInt(ROLLBACK_TIMEOUT).description("transaction rollback timeout [seconds]");

    private final TsurugiJdbcPropertyInt executeTimeout = new TsurugiJdbcPropertyInt(EXECUTE_TIMEOUT).description("transaction execute timeout [seconds]");

    private final TsurugiJdbcPropertyInt queryTimeout = new TsurugiJdbcPropertyInt(QUERY_TIMEOUT).description("SELECT timeout [seconds]");

    private final TsurugiJdbcPropertyInt defaultTimeout = new TsurugiJdbcPropertyInt(DEFAULT_TIMEOUT).description("default timeout [seconds]").defaultValue(0);

    private final TsurugiJdbcProperties properties = TsurugiJdbcProperties.of(//
            user, password, authToken, credentials, //
            applicationName, sessionLabel, keepAlive, connectTimeout, //
            shutdownType, shutdownTimeout, //
            transactionType, transactionLabel, includeDdl, writePreserve, inclusiveReadArea, exclusiveReadArea, scanParallel, //
            autoCommit, commitType, autoDispose, //
            beginTimeout, commitTimeout, rollbackTimeout, //
            executeTimeout, //
            queryTimeout, //
            defaultTimeout);

    /**
     * Put property value.
     *
     * @param factory factory
     * @param key     key
     * @param value   value
     * @throws SQLException if property value convert error occurs
     */
    @TsurugiJdbcInternal
    public void put(TsurugiJdbcFactory factory, String key, String value) throws SQLException {
        properties.put(factory, key, value);
    }

    /**
     * Put all property values.
     *
     * @param factory factory
     * @param info    properties
     * @throws SQLException if property value convert error occurs
     */
    @TsurugiJdbcInternal
    public void putAll(TsurugiJdbcFactory factory, Properties info) throws SQLException {
        properties.putAll(factory, info);
    }

    /**
     * Get internal properties.
     *
     * @return properties
     */
    @TsurugiJdbcInternal
    public TsurugiJdbcProperties getInternalProperties() {
        return this.properties;
    }

    /**
     * Set JDBC URL.
     *
     * @param url JDBC URL
     * @throws SQLException if the url is null
     */
    public void setJdbcUrl(String url) throws SQLException {
        setJdbcUrl(TsurugiJdbcFactory.getDefaultFactory(), url);
    }

    /**
     * Set JDBC URL.
     *
     * @param factory factory
     * @param url     JDBC URL
     * @throws SQLException if the url is null
     */
    @TsurugiJdbcInternal
    public void setJdbcUrl(TsurugiJdbcFactory factory, String url) throws SQLException {
        var fromConfig = TsurugiJdbcUrlParser.parse(factory, url);
        if (fromConfig != null) {
            setEndpoint(fromConfig.getEndpoint());
            properties.copyFrom(fromConfig.getInternalProperties());
        }
    }

    // Session

    /**
     * Set endpoint.
     *
     * @param endpoint endpoint
     */
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Get endpoint.
     *
     * @return endpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Set user.
     *
     * @param user user
     */
    public void setUser(String user) {
        this.user.setValue(user);
    }

    /**
     * Get user.
     *
     * @return user
     */
    public String getUser() {
        return user.value();
    }

    /**
     * Set password.
     *
     * @param password password
     */
    public void setPassword(String password) {
        this.password.setValue(password);
    }

    /**
     * Get password.
     *
     * @return password
     */
    public String getPassword() {
        return password.value();
    }

    /**
     * Set authentication token.
     *
     * @param authToken authentication token
     */
    public void setAuthToken(String authToken) {
        this.authToken.setValue(authToken);
    }

    /**
     * Get authentication token.
     *
     * @return authentication token
     */
    public String getAuthToken() {
        return authToken.value();
    }

    /**
     * Set credentials.
     *
     * @param path credential file path
     */
    public void setCredentials(String path) {
        this.credentials.setValue(path);
    }

    /**
     * Get credentials.
     *
     * @return credential file path
     */
    public String getCredentials() {
        return credentials.value();
    }

    /**
     * Get Credential.
     *
     * @param factory factory
     * @return Credential
     * @throws SQLException If credential creation fails
     */
    public Credential getCredential(TsurugiJdbcFactory factory) throws SQLException {
        String user = getUser();
        if (user != null) {
            String password = getPassword();
            return new UsernamePasswordCredential(user, password);
        }

        String authToken = getAuthToken();
        if (authToken != null) {
            return new RememberMeCredential(authToken);
        }

        String path = getCredentials();
        if (path != null) {
            try {
                return FileCredential.load(Path.of(path));
            } catch (IOException e) {
                throw factory.getExceptionHandler().credentialFileLoadException(e);
            }
        }

        return NullCredential.INSTANCE;
    }

    /**
     * Set application name.
     *
     * @param applicationName application name
     */
    public void setApplicationName(String applicationName) {
        this.applicationName.setValue(applicationName);
    }

    /**
     * Get application name.
     *
     * @return application name
     */
    public String getApplicationName() {
        return applicationName.value();
    }

    /**
     * Set session label.
     *
     * @param sessionLabel session label
     */
    public void setSessionLabel(String sessionLabel) {
        this.sessionLabel.setValue(sessionLabel);
    }

    /**
     * Get session label.
     *
     * @return session label
     */
    public String getSessionLabel() {
        return sessionLabel.value();
    }

    /**
     * Set session keep alive.
     *
     * @param keepAlive keep alive
     */
    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive.setValue(keepAlive);
    }

    /**
     * Get session keep alive.
     *
     * @return keep alive
     */
    public boolean getKeepAlive() {
        return keepAlive.value();
    }

    /**
     * Set connect timeout.
     *
     * @param timeout connect timeout [seconds]
     */
    public void setConnectTimeout(int timeout) {
        this.connectTimeout.setValue(timeout);
    }

    /**
     * Get connect timeout.
     *
     * @return connect timeout [seconds]
     */
    public int getConnectTimeout() {
        return connectTimeout.value().getAsInt();
    }

    /**
     * Set session shutdown type.
     *
     * @param shutdownType shutdown type
     */
    public void setShutdownType(TsurugiJdbcShutdownType shutdownType) {
        this.shutdownType.setValue(shutdownType);
    }

    /**
     * Get session shutdown type.
     *
     * @return shutdown type
     */
    public TsurugiJdbcShutdownType getShutdownType() {
        return shutdownType.value();
    }

    /**
     * Set session shutdown timeout.
     *
     * @param timeout shutdown timeout [seconds]
     */
    public void setShutdownTimeout(int timeout) {
        this.shutdownTimeout.setValue(timeout);
    }

    /**
     * Get session shutdown timeout.
     *
     * @return shutdown timeout [seconds]
     */
    public OptionalInt getShutdownTimeout() {
        return shutdownTimeout.value();
    }

    // Transaction

    /**
     * Set transaction type.
     *
     * @param transactionType transaction type
     */
    public void setTransactionType(TsurugiJdbcTransactionType transactionType) {
        this.transactionType.setValue(transactionType);
    }

    /**
     * Get transaction type.
     *
     * @return transaction type
     */
    public TsurugiJdbcTransactionType getTransactionType() {
        return transactionType.value();
    }

    /**
     * Set transaction label.
     *
     * @param transactionLabel transaction label
     */
    public void setTransactionLabel(String transactionLabel) {
        this.transactionLabel.setValue(transactionLabel);
    }

    /**
     * Get transaction label.
     *
     * @return transaction label
     */
    public String getTransactionLabel() {
        return transactionLabel.value();
    }

    /**
     * Set LTX include DDL.
     *
     * @param includeDdl include DDL
     */
    public void setTransactionIncludeDdl(boolean includeDdl) {
        this.includeDdl.setValue(includeDdl);
    }

    /**
     * Get LTX include DDL.
     *
     * @return include DDL
     */
    public boolean getTransactionIncludeDdl() {
        return includeDdl.value();
    }

    /**
     * Set LTX write preserve.
     *
     * @param tableNames table names
     */
    public void setWritePreserve(List<String> tableNames) {
        this.writePreserve.setValue(tableNames);
    }

    /**
     * Get LTX write preserve.
     *
     * @return table names
     */
    public List<String> getWritePreserve() {
        return writePreserve.value();
    }

    /**
     * Set LTX inclusive read area.
     *
     * @param tableNames table names
     */
    public void setInclusiveReadArea(List<String> tableNames) {
        this.inclusiveReadArea.setValue(tableNames);
    }

    /**
     * Get LTX inclusive read area.
     *
     * @return table names
     */
    public List<String> getInclusiveReadArea() {
        return inclusiveReadArea.value();
    }

    /**
     * Set LTX exclusive read area.
     *
     * @param tableNames table names
     */
    public void setExclusiveReadArea(List<String> tableNames) {
        this.exclusiveReadArea.setValue(tableNames);
    }

    /**
     * Get LTX exclusive read area.
     *
     * @return table names
     */
    public List<String> getExclusiveReadArea() {
        return exclusiveReadArea.value();
    }

    /**
     * Set RTX scan parallel.
     *
     * @param scanParallel scan parallel
     */
    public void setTransactionScanParallel(int scanParallel) {
        this.scanParallel.setValue(scanParallel);
    }

    /**
     * Get RTX scan parallel.
     *
     * @return scan parallel
     */
    public OptionalInt getScanParallel() {
        return scanParallel.value();
    }

    /**
     * Set auto commit.
     *
     * @param autoCommit auto commit
     */
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit.setValue(autoCommit);
    }

    /**
     * Get auto commit.
     *
     * @return auto commit
     */
    public boolean getAutoCommit() {
        return autoCommit.value();
    }

    /**
     * Set commit type.
     *
     * @param commitType commit type
     */
    public void setCommitType(TsurugiJdbcCommitType commitType) {
        this.commitType.setValue(commitType);
    }

    /**
     * Get commit type.
     *
     * @return commit type
     */
    public TsurugiJdbcCommitType getCommitType() {
        return commitType.value();
    }

    /**
     * Set automatically dispose upon commit.
     *
     * @param autoDispose automatically dispose
     */
    public void setCommitAutoDispose(boolean autoDispose) {
        this.autoDispose.setValue(autoDispose);
    }

    /**
     * Get automatically dispose upon commit.
     *
     * @return automatically dispose
     */
    public boolean getTransactionAutoDispose() {
        return autoDispose.value();
    }

    /**
     * Set transaction begin timeout.
     *
     * @param timeout begin timeout [seconds]
     */
    public void setBeginTimeout(int timeout) {
        this.beginTimeout.setValue(timeout);
    }

    /**
     * Get transaction begin timeout.
     *
     * @return begin timeout [seconds]
     */
    public OptionalInt getBeginTimeout() {
        return beginTimeout.value();
    }

    /**
     * Set transaction commit timeout.
     *
     * @param timeout commit timeout [seconds]
     */
    public void setCommitTimeout(int timeout) {
        this.commitTimeout.setValue(timeout);
    }

    /**
     * Get transaction commit timeout.
     *
     * @return commit timeout [seconds]
     */
    public OptionalInt getCommitTimeout() {
        return commitTimeout.value();
    }

    /**
     * Set transaction rollback timeout.
     *
     * @param timeout rollback timeout [seconds]
     */
    public void setRollbackTimeout(int timeout) {
        this.rollbackTimeout.setValue(timeout);
    }

    /**
     * Get transaction rollback timeout.
     *
     * @return rollback timeout [seconds]
     */
    public OptionalInt getRollbackTimeout() {
        return rollbackTimeout.value();
    }

    // Statement

    /**
     * Set statement execute timeout.
     *
     * @param timeout execute timeout [seconds]
     */
    public void setExecuteTimeout(int timeout) {
        this.executeTimeout.setValue(timeout);
    }

    /**
     * Get statement execute timeout.
     *
     * @return execute timeout [seconds]
     */
    public OptionalInt getExecuteTimeout() {
        return executeTimeout.value();
    }

    // ResultSet

    /**
     * Set SELECT timeout.
     *
     * @param timeout SELECT timeout [seconds]
     */
    public void setQueryTimeout(int timeout) {
        this.queryTimeout.setValue(timeout);
    }

    /**
     * Get SELECT timeout.
     *
     * @return SELECT timeout [seconds]
     */
    public OptionalInt getQueryTimeout() {
        return queryTimeout.value();
    }

    // Common

    /**
     * Set default timeout.
     *
     * @param timeout default timeout [seconds]
     */
    public void setDefaultTimeout(int timeout) {
        this.defaultTimeout.setValue(timeout);
    }

    /**
     * Get default timeout.
     *
     * @return default timeout [seconds]
     */
    public OptionalInt getDefaultTimeout() {
        return defaultTimeout.value();
    }
}
