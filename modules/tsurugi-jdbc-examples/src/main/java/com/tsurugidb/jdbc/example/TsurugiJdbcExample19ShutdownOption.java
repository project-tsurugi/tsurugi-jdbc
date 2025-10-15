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
import com.tsurugidb.jdbc.connection.TsurugiJdbcShutdownType;

public class TsurugiJdbcExample19ShutdownOption {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample19ShutdownOption.class);

    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        defaultShutdownType();
        connect1();
        connect2();
        connection_setShutdownType();
        connection_setClientInfo();
    }

    static void defaultShutdownType() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            printShutdownType("default", connection); // GRACEFUL
        }
    }

    static void connect1() throws SQLException {
        String url = JDBC_URL //
                + encode("?", TsurugiConfig.SHUTDOWN_TYPE, "FORCEFUL");

        try (var connection = DriverManager.getConnection(url, "tsurugi", "password")) {
            printShutdownType("connect1", connection);
        }
    }

    static void connect2() throws SQLException {
        var info = new Properties();
        info.setProperty(TsurugiConfig.USER, "tsurugi");
        info.setProperty(TsurugiConfig.PASSWORD, "password");
        info.setProperty(TsurugiConfig.SHUTDOWN_TYPE, TsurugiJdbcShutdownType.FORCEFUL.name());

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
            printShutdownType("connect2", connection);
        }
    }

    static void connection_setShutdownType() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            TsurugiJdbcConnection rawConnection = connection.unwrap(TsurugiJdbcConnection.class);
            rawConnection.setShutdownType(TsurugiJdbcShutdownType.FORCEFUL);

            printShutdownType("setShutdownType", connection);
        }
    }

    static void connection_setClientInfo() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            connection.setClientInfo(TsurugiConfig.SHUTDOWN_TYPE, TsurugiJdbcShutdownType.FORCEFUL.name());

            printShutdownType("setClientInfo", connection);
        }
    }

    private static void printShutdownType(String message, Connection connection) throws SQLException {
        TsurugiJdbcConnection rawConnection = connection.unwrap(TsurugiJdbcConnection.class);
        TsurugiJdbcShutdownType type = rawConnection.getShutdownType();

        LOG.info("{} - shutdownType={}", message, type);
    }

    private static String encode(String prefix, String key, String value) {
        return prefix + URLEncoder.encode(key, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
