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
package com.tsurugidb.jdbc.test.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.TsurugiDataSource;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnectionBuilder;
import com.tsurugidb.jdbc.test.util.JdbcDbTestConnector;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * {@link TsurugiJdbcConnectionBuilder} connect test.
 */
public class JdbcDbConnectionBuilderTest extends JdbcDbTester {

    @Test
    void connect() throws SQLException {
        var builder = new TsurugiDataSource().createConnectionBuilder() //
                .endpoint(JdbcDbTestConnector.getEndPoint());
        JdbcDbTestConnector.setCredentialTo(builder);

        try (var connection = builder.build()) {
        }
    }

    @Test
    void connectInherit() throws SQLException {
        var ds = new TsurugiDataSource();
        ds.setEndpoint(JdbcDbTestConnector.getEndPoint());
        JdbcDbTestConnector.setCredentialTo(ds);

        var builder = ds.createConnectionBuilder();
        try (var connection = builder.build()) {
        }
    }

    @Test
    void nullCredential() throws SQLException {
        boolean enableNullCredential = JdbcDbTestConnector.enableNullCredential();

        var builder = new TsurugiDataSource().createConnectionBuilder() //
                .endpoint(JdbcDbTestConnector.getEndPoint());

        if (enableNullCredential) {
            try (var connection = builder.build()) {
            }
        } else {
            var e = assertThrows(SQLInvalidAuthorizationSpecException.class, () -> {
                try (var connection = builder.build()) {
                }
            });
            assertEquals("28000", e.getSQLState());
        }
    }

    @Test
    void userPassword() throws SQLException {
        String user = JdbcDbTestConnector.getUser();
        assumeTrue(user != null, "user is not specified");
        String password = JdbcDbTestConnector.getPassword();

        var builder = new TsurugiDataSource().createConnectionBuilder() //
                .endpoint(JdbcDbTestConnector.getEndPoint()) //
                .user(user).password(password);

        try (var connection = builder.build()) {
        }
    }

    @Test
    void authToken() throws SQLException {
        String token = JdbcDbTestConnector.getAuthToken();
        assumeTrue(token != null, "authToken is not specified");

        var builder = new TsurugiDataSource().createConnectionBuilder() //
                .endpoint(JdbcDbTestConnector.getEndPoint()) //
                .authToken(token);

        try (var connection = builder.build()) {
        }
    }

    @Test
    void credentialFile() throws SQLException {
        String path = JdbcDbTestConnector.getCredentials();
        assumeTrue(path != null, "credentials is not specified");

        var builder = new TsurugiDataSource().createConnectionBuilder() //
                .endpoint(JdbcDbTestConnector.getEndPoint()) //
                .credentials(path);

        try (var connection = builder.build()) {
        }
    }
}
