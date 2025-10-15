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
package com.tsurugidb.jdbc.example;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.TsurugiDataSource;

public class TsurugiJdbcExample11Credential {
    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        connect_userPassword3();
    }

    static void connect_userPassword1() throws SQLException {
        String url = JDBC_URL //
                + encode("?", TsurugiConfig.USER, "tsurugi") //
                + encode("&", TsurugiConfig.PASSWORD, "password");

        try (var connection = DriverManager.getConnection(url)) {
        }
    }

    static void connect_userPassword2() throws SQLException {
        var info = new Properties();
        info.setProperty(TsurugiConfig.USER, "tsurugi");
        info.setProperty(TsurugiConfig.PASSWORD, "password");

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
        }
    }

    static void connect_userPassword3() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
        }
    }

    static void connect_authToken1() throws SQLException {
        String url = JDBC_URL //
                + encode("?", TsurugiConfig.AUTH_TOKEN, "...token...");

        try (var connection = DriverManager.getConnection(url)) {
        }
    }

    static void connect_authToken2() throws SQLException {
        var info = new Properties();
        info.setProperty(TsurugiConfig.AUTH_TOKEN, "...token...");

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
        }
    }

    static void connect_credentialFile1() throws SQLException {
        String url = JDBC_URL //
                + encode("?", TsurugiConfig.CREDENTIALS, "/path/to/credentials.key");

        try (var connection = DriverManager.getConnection(url)) {
        }
    }

    static void connect_credentialFile2() throws SQLException {
        var info = new Properties();
        info.setProperty(TsurugiConfig.CREDENTIALS, "/path/to/credentials.key");

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
        }
    }

    static void dataSource_userPassword1() throws SQLException {
        var dataSource = new TsurugiDataSource();
        dataSource.setEndpoint("tcp://localhost:12345");
        dataSource.setUser("tsurugi");
        dataSource.setPassword("password");

        try (var connection = dataSource.getConnection()) {
        }
    }

    static void dataSource_userPassword2() throws SQLException {
        var config = new TsurugiConfig();
        config.setEndpoint("tcp://localhost:12345");
        config.setUser("tsurugi");
        config.setPassword("password");
        var dataSource = new TsurugiDataSource(config);

        try (var connection = dataSource.getConnection()) {
        }
    }

    static void dataSource_userPassword3() throws SQLException {
        var dataSource = new TsurugiDataSource();
        dataSource.setEndpoint("tcp://localhost:12345");

        try (var connection = dataSource.getConnection("tsurugi", "password")) {
        }
    }

    static void dataSource_authToken1() throws SQLException {
        var dataSource = new TsurugiDataSource();
        dataSource.setEndpoint("tcp://localhost:12345");
        dataSource.setAuthToken("...token...");

        try (var connection = dataSource.getConnection()) {
        }
    }

    static void dataSource_authToken2() throws SQLException {
        var config = new TsurugiConfig();
        config.setEndpoint("tcp://localhost:12345");
        config.setAuthToken("...token...");
        var dataSource = new TsurugiDataSource(config);

        try (var connection = dataSource.getConnection()) {
        }
    }

    static void dataSource_credentialFile1() throws SQLException {
        var dataSource = new TsurugiDataSource();
        dataSource.setEndpoint("tcp://localhost:12345");
        dataSource.setCredentials("/path/to/credentials.key");

        try (var connection = dataSource.getConnection()) {
        }
    }

    static void dataSource_credentialFile2() throws SQLException {
        var config = new TsurugiConfig();
        config.setEndpoint("tcp://localhost:12345");
        config.setCredentials("/path/to/credentials.key");
        var dataSource = new TsurugiDataSource(config);

        try (var connection = dataSource.getConnection()) {
        }
    }

    static void builder_userPassword() throws SQLException {
        var dataSource = new TsurugiDataSource();
        var builder = dataSource.createConnectionBuilder() //
                .endpoint("tcp://localhost:12345") //
                .user("tsurugi") //
                .password("password") //
        ;

        try (var connection = builder.build()) {
        }
    }

    static void builder_authToken() throws SQLException {
        var dataSource = new TsurugiDataSource();
        var builder = dataSource.createConnectionBuilder() //
                .endpoint("tcp://localhost:12345") //
                .authToken("...token...") //
        ;

        try (var connection = builder.build()) {
        }
    }

    static void builder_credentialFile() throws SQLException {
        var dataSource = new TsurugiDataSource();
        var builder = dataSource.createConnectionBuilder() //
                .endpoint("tcp://localhost:12345") //
                .credentials("/path/to/credentials.key") //
        ;

        try (var connection = builder.build()) {
        }
    }

    private static String encode(String prefix, String key, String value) {
        return prefix + URLEncoder.encode(key, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
