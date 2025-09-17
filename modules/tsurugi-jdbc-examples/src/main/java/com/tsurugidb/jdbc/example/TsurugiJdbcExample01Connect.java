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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class TsurugiJdbcExample01Connect {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample01Connect.class);

    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        acceptUrl();
        connect2();
    }

    static void acceptUrl() throws SQLException {
        var driver = DriverManager.getDriver("jdbc:tsurugi:hoge");
        LOG.info("driver={}", driver);
    }

    static void connect0() throws SQLException {
        LOG.info("connect0 start");

        try (var connection = DriverManager.getConnection(JDBC_URL)) {
        }

        LOG.info("connect0 end");
    }

    static void connect1() throws SQLException {
        LOG.info("connect1 start");

        String url = JDBC_URL //
                + encode("?", "user", "tsurugi") //
                + encode("&", "password", "password");

        try (var connection = DriverManager.getConnection(url)) {
        }

        LOG.info("connect1 end");
    }

    private static String encode(String prefix, String key, String value) {
        return prefix + URLEncoder.encode(key, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    static void connect2() throws SQLException {
        LOG.info("connect2 start");

        try (var connection = DriverManager.getConnection(JDBC_URL, "tsurugi", "password")) {
        }

        LOG.info("connect2 end");
    }

    static void connect3() throws SQLException {
        LOG.info("connect3 start");

        var info = new Properties();
        info.setProperty("user", "tsurugi");
        info.setProperty("password", "password");
        info.setProperty("connectTimeout", "10"); // seconds

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
        }

        LOG.info("connect3 end");
    }
}
