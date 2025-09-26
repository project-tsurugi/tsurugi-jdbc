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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.DriverManager;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.test.util.JdbcDbTestConnector;
import com.tsurugidb.jdbc.test.util.JdbcDbTestCredential;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * TsurugiDriver connect test.
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
