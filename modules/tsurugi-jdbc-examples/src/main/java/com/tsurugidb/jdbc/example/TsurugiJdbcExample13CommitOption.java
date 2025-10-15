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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcCommitType;

public class TsurugiJdbcExample13CommitOption {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample13CommitOption.class);

    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        defaultCommitType();
        connect1();
        connect2();
        connection_setCommitType();
        connection_setClientInfo();
    }

    static void defaultCommitType() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            printCommitType("default", connection); // DEFAULT
        }
    }

    static void connect1() throws SQLException {
        String url = JDBC_URL //
                + encode("?", TsurugiConfig.COMMIT_TYPE, "STORED") //
                + encode("&", TsurugiConfig.AUTO_DISPOSE, "false");

        try (var connection = DriverManager.getConnection(url, "tsurugi", "password")) {
            printCommitType("connect1", connection);
        }
    }

    static void connect2() throws SQLException {
        var info = new Properties();
        info.setProperty(TsurugiConfig.USER, "tsurugi");
        info.setProperty(TsurugiConfig.PASSWORD, "password");
        info.setProperty(TsurugiConfig.COMMIT_TYPE, TsurugiJdbcCommitType.STORED.name());
        info.setProperty(TsurugiConfig.AUTO_DISPOSE, Boolean.FALSE.toString());

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
            printCommitType("connect2", connection);
        }
    }

    static void connection_setCommitType() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            TsurugiJdbcConnection rawConnection = connection.unwrap(TsurugiJdbcConnection.class);
            rawConnection.setCommitType(TsurugiJdbcCommitType.STORED);
            rawConnection.setCommitAutoDispose(false);

            printCommitType("setCommitType", connection);
        }
    }

    static void connection_setClientInfo() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            connection.setClientInfo(TsurugiConfig.COMMIT_TYPE, TsurugiJdbcCommitType.STORED.name());
            connection.setClientInfo(TsurugiConfig.AUTO_DISPOSE, "false");

            printCommitType("setClientInfo", connection);
        }
    }

    private static void printCommitType(String message, Connection connection) throws SQLException {
        TsurugiJdbcConnection rawConnection = connection.unwrap(TsurugiJdbcConnection.class);
        TsurugiJdbcCommitType type = rawConnection.getCommitType();

        LOG.info("{} - commitType={}", message, type);
    }

    private static String encode(String prefix, String key, String value) {
        return prefix + URLEncoder.encode(key, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
