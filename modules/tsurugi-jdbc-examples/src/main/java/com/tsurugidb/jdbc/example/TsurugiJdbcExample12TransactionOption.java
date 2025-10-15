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
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;
import com.tsurugidb.jdbc.transaction.TsurugiJdbcTransactionType;

public class TsurugiJdbcExample12TransactionOption {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample12TransactionOption.class);

    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        defaultTransactionType();
        connect1();
        connect2();
        connection_setTransactionType();
        connection_setClientInfo();
        connection_setReadOnly();
    }

    static void defaultTransactionType() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            printTransactionType("default", connection); // OCC
        }
    }

    static void connect1() throws SQLException {
        String url = JDBC_URL //
                + encode("?", TsurugiConfig.TRANSACTION_TYPE, "LTX") //
                + encode("&", TsurugiConfig.WRITE_PRESERVE, "test1, test2");

        try (var connection = DriverManager.getConnection(url, "tsurugi", "password")) {
            printTransactionType("connect1", connection); // LTX
        }
    }

    static void connect2() throws SQLException {
        var info = new Properties();
        info.setProperty(TsurugiConfig.USER, "tsurugi");
        info.setProperty(TsurugiConfig.PASSWORD, "password");
        info.setProperty(TsurugiConfig.TRANSACTION_TYPE, TsurugiJdbcTransactionType.LTX.name());
        info.setProperty(TsurugiConfig.WRITE_PRESERVE, "test1, test2");

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
            printTransactionType("connect2", connection); // LTX
        }
    }

    static void connection_setTransactionType() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            TsurugiJdbcConnection rawConnection = connection.unwrap(TsurugiJdbcConnection.class);
            rawConnection.setTransactionType(TsurugiJdbcTransactionType.LTX);
            rawConnection.setWritePreserve(List.of("test1", "test2"));

            printTransactionType("setTransactionType", connection); // LTX
        }
    }

    static void connection_setClientInfo() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            connection.setClientInfo(TsurugiConfig.TRANSACTION_TYPE, TsurugiJdbcTransactionType.LTX.name());
            connection.setClientInfo(TsurugiConfig.WRITE_PRESERVE, "test1, test2");

            printTransactionType("setClientInfo", connection); // LTX
        }
    }

    static void connection_setReadOnly() throws SQLException {
        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
            connection.setReadOnly(true);
            printTransactionType("readOnly=true ", connection); // RTX

            connection.setReadOnly(false);
            printTransactionType("readOnly=false", connection); // OCC
        }
    }

    private static void printTransactionType(String message, Connection connection) throws SQLException {
        TsurugiJdbcConnection rawConnection = connection.unwrap(TsurugiJdbcConnection.class);
        TsurugiJdbcTransactionType type = rawConnection.getTransactionType();

        LOG.info("{} - transactionType={}", message, type);
    }

    private static String encode(String prefix, String key, String value) {
        return prefix + URLEncoder.encode(key, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
