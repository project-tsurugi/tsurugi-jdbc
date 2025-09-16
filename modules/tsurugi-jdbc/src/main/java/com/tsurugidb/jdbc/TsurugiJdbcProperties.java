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
import java.sql.SQLException;
import java.util.OptionalInt;
import java.util.Properties;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.connection.TsurugiJdbcShutdownType;
import com.tsurugidb.jdbc.connection.TsurugiJdbcTransactionType;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.property.TsurugiJdbcInternalProperties;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyBoolean;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyEnum;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyInt;
import com.tsurugidb.jdbc.property.TsurugiJdbcPropertyString;
import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

public class TsurugiJdbcProperties {
    // Session
    public static final String ENDPOINT = "endpoint";
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
    public static final String AUTO_COMMIT = "autoCommit";
    public static final String BEGIN_TIMEOUT = "beginTimeout";
    public static final String COMMIT_TIMEOUT = "commitTimeout";
    public static final String ROLLBACK_TIMEOUT = "rollbackTimeout";
    // Statement
    public static final String EXECUTE_TIMEOUT = "executeTimeout";
    // ResultSet
    public static final String QUERY_TIMEOUT = "queryTimeout";
    // Common
    public static final String DEFAULT_TIMEOUT = "defaultTimeout";

    private final TsurugiJdbcPropertyString endpoint = new TsurugiJdbcPropertyString(ENDPOINT);
    private final TsurugiJdbcPropertyString user = new TsurugiJdbcPropertyString(USER);
    private final TsurugiJdbcPropertyString password = new TsurugiJdbcPropertyString(PASSWORD);
    private final TsurugiJdbcPropertyString authToken = new TsurugiJdbcPropertyString(AUTH_TOKEN);
    private final TsurugiJdbcPropertyString credentials = new TsurugiJdbcPropertyString(CREDENTIALS);
    private final TsurugiJdbcPropertyString applicationName = new TsurugiJdbcPropertyString(APPLICATION_NAME);
    private final TsurugiJdbcPropertyString sessionLabel = new TsurugiJdbcPropertyString(SESSION_LABEL);
    private final TsurugiJdbcPropertyBoolean keepAlive = new TsurugiJdbcPropertyBoolean(KEEP_ALIVE);
    private final TsurugiJdbcPropertyInt connectTimeout = new TsurugiJdbcPropertyInt(CONNECT_TIMEOUT);
    private final TsurugiJdbcPropertyEnum<TsurugiJdbcShutdownType> shutdownType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcShutdownType.class, SHUTDOWN_TYPE);
    private final TsurugiJdbcPropertyInt shutdownTimeout = new TsurugiJdbcPropertyInt(SHUTDOWN_TIMEOUT);

    private final TsurugiJdbcPropertyEnum<TsurugiJdbcTransactionType> transactionType = new TsurugiJdbcPropertyEnum<>(TsurugiJdbcTransactionType.class, TRANSACTION_TYPE);
    private final TsurugiJdbcPropertyString transactionLabel = new TsurugiJdbcPropertyString(TRANSACTION_LABEL);
    private final TsurugiJdbcPropertyBoolean autoCommit = new TsurugiJdbcPropertyBoolean(AUTO_COMMIT);
    private final TsurugiJdbcPropertyInt beginTimeout = new TsurugiJdbcPropertyInt(BEGIN_TIMEOUT);
    private final TsurugiJdbcPropertyInt commitTimeout = new TsurugiJdbcPropertyInt(COMMIT_TIMEOUT);
    private final TsurugiJdbcPropertyInt rollbackTimeout = new TsurugiJdbcPropertyInt(ROLLBACK_TIMEOUT);

    private final TsurugiJdbcPropertyInt executeTimeout = new TsurugiJdbcPropertyInt(EXECUTE_TIMEOUT);

    private final TsurugiJdbcPropertyInt queryTimeout = new TsurugiJdbcPropertyInt(QUERY_TIMEOUT);

    private final TsurugiJdbcPropertyInt defaultTimeout = new TsurugiJdbcPropertyInt(DEFAULT_TIMEOUT);

    private final TsurugiJdbcInternalProperties properties = TsurugiJdbcInternalProperties.of( //
            endpoint, //
            user, password, authToken, credentials, //
            applicationName, sessionLabel, keepAlive, connectTimeout, //
            shutdownType, shutdownTimeout, //
            transactionType, transactionLabel, autoCommit, //
            beginTimeout, commitTimeout, rollbackTimeout, //
            executeTimeout, //
            queryTimeout, //
            defaultTimeout);

    public void put(TsurugiJdbcFactory factory, String key, String value) throws SQLException {
        properties.put(factory, key, value);
    }

    public void putAll(TsurugiJdbcFactory factory, Properties info) throws SQLException {
        properties.putAll(factory, info);
    }

    @TsurugiJdbcInternal
    public TsurugiJdbcInternalProperties getInternalProperties() {
        return this.properties;
    }

    // Session

    public void setEndpoint(String endpoint) {
        this.endpoint.setStringValue(endpoint);
    }

    public String getEndpoint() {
        return endpoint.value();
    }

    public String getUser() {
        return user.value();
    }

    public String getPassword() {
        return password.value();
    }

    public String getAuthToken() {
        return authToken.value();
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

    public String getApplicationName() {
        return applicationName.value();
    }

    public String getSessionLabel() {
        return sessionLabel.value();
    }

    public boolean getKeepAlive() {
        return keepAlive.value(true);
    }

    public int getConnectTimeout(int defaultTimeout) {
        var value = connectTimeout.value();
        if (value.isPresent()) {
            return value.getAsInt();
        }
        return getDefaultTimeout().orElse(defaultTimeout);
    }

    // Common

    public OptionalInt getDefaultTimeout() {
        return defaultTimeout.value();
    }
}
