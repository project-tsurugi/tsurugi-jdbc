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
import com.tsurugidb.jdbc.property.TsurugiJdbcInternalProperties;
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

public class TsurugiJdbcProperties {
    // Session
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String AUTH_TOKEN = "authToken";
    public static final String CREDENTIALS = "credentials";
    public static final String APPLICATION_NAME = "applicationName";
    public static final String SESSION_LABEL = "sessionLabel";
    public static final String KEEP_ALIVE = "keepAlive";
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    public static final String SHUTDOWN_TYPE = "shutdownType";
    public static final String SHUTDOWN_TIMEOUT = "shutdownTimeout";
    // Transaction
    public static final String TRANSACTION_TYPE = "transactionType";
    public static final String TRANSACTION_LABEL = "transactionLabel";
    public static final String INCLUDE_DDL = "includeDdl";
    public static final String WRITE_PRESERVE = "writePreserve";
    public static final String INCLUSIVE_READ_AREA = "inclusiveReadArea";
    public static final String EXCLUSIVE_READ_AREA = "exclusiveReadArea";
    public static final String SCAN_PARALLEL = "scanParallel";
    public static final String AUTO_COMMIT = "autoCommit";
    public static final String COMMIT_TYPE = "commitType";
    public static final String AUTO_DISPOSE = "autoDispose";
    public static final String BEGIN_TIMEOUT = "beginTimeout";
    public static final String COMMIT_TIMEOUT = "commitTimeout";
    public static final String ROLLBACK_TIMEOUT = "rollbackTimeout";
    // Statement
    public static final String EXECUTE_TIMEOUT = "executeTimeout";
    // ResultSet
    public static final String QUERY_TIMEOUT = "queryTimeout";
    // Common
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

    private final TsurugiJdbcInternalProperties properties = TsurugiJdbcInternalProperties.of( //
            user, password, authToken, credentials, //
            applicationName, sessionLabel, keepAlive, connectTimeout, //
            shutdownType, shutdownTimeout, //
            transactionType, transactionLabel, includeDdl, writePreserve, inclusiveReadArea, exclusiveReadArea, scanParallel, //
            autoCommit, commitType, autoDispose, //
            beginTimeout, commitTimeout, rollbackTimeout, //
            executeTimeout, //
            queryTimeout, //
            defaultTimeout);

    @TsurugiJdbcInternal
    public void put(TsurugiJdbcFactory factory, String key, String value) throws SQLException {
        properties.put(factory, key, value);
    }

    @TsurugiJdbcInternal
    public void putAll(TsurugiJdbcFactory factory, Properties info) throws SQLException {
        properties.putAll(factory, info);
    }

    @TsurugiJdbcInternal
    public TsurugiJdbcInternalProperties getInternalProperties() {
        return this.properties;
    }

    public void setJdbcUrl(String url) throws SQLException {
        setJdbcUrl(TsurugiJdbcFactory.getDefaultFactory(), url);
    }

    @TsurugiJdbcInternal
    public void setJdbcUrl(TsurugiJdbcFactory factory, String url) throws SQLException {
        var fromProperties = TsurugiJdbcUrlParser.parse(factory, url);
        if (fromProperties != null) {
            setEndpoint(fromProperties.getEndpoint());
            properties.copyFrom(fromProperties.getInternalProperties());
        }
    }

    // Session

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setUser(String user) {
        this.user.setValue(user);
    }

    public String getUser() {
        return user.value();
    }

    public void setPassword(String password) {
        this.password.setValue(password);
    }

    public String getPassword() {
        return password.value();
    }

    public void setAuthToken(String authToken) {
        this.authToken.setValue(authToken);
    }

    public String getAuthToken() {
        return authToken.value();
    }

    public void setCredentials(String path) {
        this.credentials.setValue(path);
    }

    public String getCredentials() {
        return credentials.value();
    }

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

    public void setApplicationName(String applicationName) {
        this.applicationName.setValue(applicationName);
    }

    public String getApplicationName() {
        return applicationName.value();
    }

    public void setSessionLabel(String sessionLabel) {
        this.sessionLabel.setValue(sessionLabel);
    }

    public String getSessionLabel() {
        return sessionLabel.value();
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive.setValue(keepAlive);
    }

    public boolean getKeepAlive() {
        return keepAlive.value();
    }

    public void setConnectTimeout(int timeout) {
        this.connectTimeout.setValue(timeout);
    }

    public int getConnectTimeout() {
        return connectTimeout.value().getAsInt();
    }

    public void setShutdownType(TsurugiJdbcShutdownType shutdownType) {
        this.shutdownType.setValue(shutdownType);
    }

    public TsurugiJdbcShutdownType getShutdownType() {
        return shutdownType.value();
    }

    public void setShutdownTimeout(int timeout) {
        this.shutdownTimeout.setValue(timeout);
    }

    public OptionalInt getShutdownTimeout() {
        return shutdownTimeout.value();
    }

    // Transaction

    public void setTransactionType(TsurugiJdbcTransactionType transactionType) {
        this.transactionType.setValue(transactionType);
    }

    public TsurugiJdbcTransactionType getTransactionType() {
        return transactionType.value();
    }

    public void setTransactionLabel(String transactionLabel) {
        this.transactionLabel.setValue(transactionLabel);
    }

    public String getTransactionLabel() {
        return transactionLabel.value();
    }

    public void setTransactionIncludeDdl(boolean includeDdl) {
        this.includeDdl.setValue(includeDdl);
    }

    public boolean getTransactionIncludeDdl() {
        return includeDdl.value();
    }

    public void setWritePreserve(List<String> tableNames) {
        this.writePreserve.setValue(tableNames);
    }

    public List<String> getWritePreserve() {
        return writePreserve.value();
    }

    public void setInclusiveReadArea(List<String> tableNames) {
        this.inclusiveReadArea.setValue(tableNames);
    }

    public List<String> getInclusiveReadArea() {
        return inclusiveReadArea.value();
    }

    public void setExclusiveReadArea(List<String> tableNames) {
        this.exclusiveReadArea.setValue(tableNames);
    }

    public List<String> getExclusiveReadArea() {
        return exclusiveReadArea.value();
    }

    public void setTransactionScanParallel(int scanParallel) {
        this.scanParallel.setValue(scanParallel);
    }

    public OptionalInt getScanParallel() {
        return scanParallel.value();
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit.setValue(autoCommit);
    }

    public boolean getAutoCommit() {
        return autoCommit.value();
    }

    public void setCommitType(TsurugiJdbcCommitType commitType) {
        this.commitType.setValue(commitType);
    }

    public TsurugiJdbcCommitType getCommitType() {
        return commitType.value();
    }

    public void setCommitAutoDispose(boolean autoDispose) {
        this.autoDispose.setValue(autoDispose);
    }

    public boolean getTransactionAutoDispose() {
        return autoDispose.value();
    }

    public void setBeginTimeout(int timeout) {
        this.beginTimeout.setValue(timeout);
    }

    public OptionalInt getBeginTimeout() {
        return beginTimeout.value();
    }

    public void setCommitTimeout(int timeout) {
        this.commitTimeout.setValue(timeout);
    }

    public OptionalInt getCommitTimeout() {
        return commitTimeout.value();
    }

    public void setRollbackTimeout(int timeout) {
        this.rollbackTimeout.setValue(timeout);
    }

    public OptionalInt getRollbackTimeout() {
        return rollbackTimeout.value();
    }

    // Statement

    public void setExecuteTimeout(int timeout) {
        this.executeTimeout.setValue(timeout);
    }

    public OptionalInt getExecuteTimeout() {
        return executeTimeout.value();
    }

    // ResultSet

    public void setQueryTimeout(int timeout) {
        this.queryTimeout.setValue(timeout);
    }

    public OptionalInt getQueryTimeout() {
        return queryTimeout.value();
    }

    // Common

    public void setDefaultTimeout(int defaultTimeout) {
        this.defaultTimeout.setValue(defaultTimeout);
    }

    public OptionalInt getDefaultTimeout() {
        return defaultTimeout.value();
    }
}
