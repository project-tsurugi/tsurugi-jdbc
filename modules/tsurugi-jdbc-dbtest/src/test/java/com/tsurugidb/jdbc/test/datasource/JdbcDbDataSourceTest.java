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
package com.tsurugidb.jdbc.test.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.sql.SQLInvalidAuthorizationSpecException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.TsurugiDataSource;
import com.tsurugidb.jdbc.test.util.JdbcDbTestConnector;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;

/**
 * TsurugiDataSource connect test.
 */
public class JdbcDbDataSourceTest extends JdbcDbTester {

    @Test
    void connect() throws Exception {
        var ds = new TsurugiDataSource();
        ds.setEndpoint(JdbcDbTestConnector.getEndPoint());
        JdbcDbTestConnector.setCredentialTo(ds);

        try (var connection = ds.getConnection()) {
        }
    }

    @Test
    void connectConfig() throws Exception {
        var config = new TsurugiConfig();
        config.setEndpoint(JdbcDbTestConnector.getEndPoint());
        JdbcDbTestConnector.setCredentialTo(config);
        var ds = new TsurugiDataSource(config);

        try (var connection = ds.getConnection()) {
        }
    }

    @Test
    void nullCredential() throws Exception {
        boolean enableNullCredential = enableNullCredential();

        var ds = new TsurugiDataSource();
        ds.setEndpoint(JdbcDbTestConnector.getEndPoint());

        if (enableNullCredential) {
            try (var connection = ds.getConnection()) {
            }
        } else {
            var e = assertThrows(SQLInvalidAuthorizationSpecException.class, () -> {
                try (var connection = ds.getConnection()) {
                }
            });
            assertEquals("28000", e.getSQLState());
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

        var ds = new TsurugiDataSource();
        ds.setEndpoint(JdbcDbTestConnector.getEndPoint());

        try (var connection = ds.getConnection(user, password)) {
        }

        ds.setUser(user);
        ds.setPassword(password);
        try (var connection = ds.getConnection()) {
        }
    }

    @Test
    void authToken() throws Exception {
        String token = JdbcDbTestConnector.getAuthToken();
        assumeTrue(token != null, "authToken is not specified");

        var ds = new TsurugiDataSource();
        ds.setEndpoint(JdbcDbTestConnector.getEndPoint());
        ds.setAuthToken(token);

        try (var connection = ds.getConnection()) {
        }
    }

    @Test
    void credentialFile() throws Exception {
        String path = JdbcDbTestConnector.getCredentials();
        assumeTrue(path != null, "credentials is not specified");

        var ds = new TsurugiDataSource();
        ds.setEndpoint(JdbcDbTestConnector.getEndPoint());
        ds.setCredentials(path);
        ;

        try (var connection = ds.getConnection()) {
        }
    }
}
