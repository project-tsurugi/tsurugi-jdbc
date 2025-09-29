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
package com.tsurugidb.jdbc.test.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.jdbc.TsurugiDriver;
import com.tsurugidb.jdbc.test.util.JdbcDbTestConnector;
import com.tsurugidb.jdbc.test.util.JdbcDbTestCredential;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;

/**
 * {@link TsurugiDriver} connect test.
 */
public class JdbcDbDriverManagerTest extends JdbcDbTester {

    @Test
    void connect() throws Exception {
        String url = JdbcDbTestConnector.getJdbcUrlWithCredential();
        try (var connection = DriverManager.getConnection(url)) {
        }
    }

    @Test
    void connectProperties() throws Exception {
        String url = JdbcDbTestConnector.getJdbcUrl();
        Properties info = JdbcDbTestConnector.getConnectProperties();
        try (var connection = DriverManager.getConnection(url, info)) {
        }
    }

    @Test
    void nullCredential() throws Exception {
        boolean enableNullCredential = enableNullCredential();

        String url = JdbcDbTestConnector.getJdbcUrl();
        if (enableNullCredential) {
            try (var connection = DriverManager.getConnection(url)) {
            }

            try (var connection = DriverManager.getConnection(url, new Properties())) {
            }
        } else {
            {
                var e = assertThrows(SQLInvalidAuthorizationSpecException.class, () -> {
                    try (var connection = DriverManager.getConnection(url)) {
                    }
                });
                assertEquals("28000", e.getSQLState());
            }
            {
                var e = assertThrows(SQLInvalidAuthorizationSpecException.class, () -> {
                    try (var connection = DriverManager.getConnection(url, new Properties())) {
                    }
                });
                assertEquals("28000", e.getSQLState());
            }
        }
    }

    private boolean enableNullCredential() throws IOException, InterruptedException {
        String endpoint = JdbcDbTestConnector.getEndPoint();
        var connector = TsurugiConnector.of(endpoint, NullCredential.INSTANCE);
        try (var session = connector.createSession()) {
            session.getLowSession();
            return true;
        } catch (TsurugiIOException e) {
            if (e.getDiagnosticCode() == CoreServiceCode.AUTHENTICATION_ERROR) {
                return false;
            }
            throw e;
        }
    }

    @Test
    void userPassword() throws Exception {
        String user = JdbcDbTestConnector.getUser();
        assumeTrue(user != null, "user is not specified");
        String password = JdbcDbTestConnector.getPassword();

        String url = JdbcDbTestConnector.getJdbcUrl();
        try (var connection = DriverManager.getConnection(url, user, password)) {
        }

        var credential = new JdbcDbTestCredential();
        credential.setUser(user);
        credential.setPassword(password);

        try (var connection = DriverManager.getConnection(url + credential.toQueryString())) {
        }

        try (var connection = DriverManager.getConnection(url, credential.toProperties())) {
        }
    }

    @Test
    void authToken() throws Exception {
        String token = JdbcDbTestConnector.getAuthToken();
        assumeTrue(token != null, "authToken is not specified");

        String url = JdbcDbTestConnector.getJdbcUrl();

        var credential = new JdbcDbTestCredential();
        credential.setAuthToken(token);

        try (var connection = DriverManager.getConnection(url + credential.toQueryString())) {
        }

        try (var connection = DriverManager.getConnection(url, credential.toProperties())) {
        }
    }

    @Test
    void credentialFile() throws Exception {
        String path = JdbcDbTestConnector.getCredentials();
        assumeTrue(path != null, "credentials is not specified");

        String url = JdbcDbTestConnector.getJdbcUrl();

        var credential = new JdbcDbTestCredential();
        credential.setCredentials(path);

        try (var connection = DriverManager.getConnection(url + credential.toQueryString())) {
        }

        try (var connection = DriverManager.getConnection(url, credential.toProperties())) {
        }
    }
}
