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
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.tsurugidb.jdbc.TsurugiConfig;
import com.tsurugidb.jdbc.TsurugiDataSource;

public class TsurugiJdbcExample02DataSource {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample02DataSource.class);

    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        connect1();
    }

    static void connect0() throws SQLException {
        LOG.info("connect0 start");

        var dataSource = new TsurugiDataSource();
        dataSource.setEndpoint("tcp://localhost:12345");

        try (var connection = dataSource.getConnection()) {
        }

        LOG.info("connect0 end");
    }

    static void connect1() throws SQLException {
        LOG.info("connect1 start");

        var dataSource = new TsurugiDataSource();
        dataSource.setEndpoint("tcp://localhost:12345");
        dataSource.setUser("tsurugi");
        dataSource.setPassword("password");

        try (var connection = dataSource.getConnection()) {
        }

        LOG.info("connect1 end");
    }

    static void connect2() throws SQLException {
        LOG.info("connect2 start");

        var dataSource = new TsurugiDataSource();
        dataSource.setEndpoint("tcp://localhost:12345");

        try (var connection = dataSource.getConnection("tsurugi", "password")) {
        }

        LOG.info("connect2 end");
    }

    static void connect3() throws SQLException {
        LOG.info("connect3 start");

        var config = new TsurugiConfig();
        config.setEndpoint("tcp://localhost:12345");
        config.setUser("tsurugi");
        config.setPassword("password");
        var dataSource = new TsurugiDataSource(config);

        try (var connection = dataSource.getConnection()) {
        }

        LOG.info("connect3 end");
    }

    static void connect4() throws SQLException {
        LOG.info("connect4 start");

        String url = JDBC_URL //
                + encode("?", "user", "tsurugi") //
                + encode("&", "password", "password");

        var dataSource = new TsurugiDataSource();
        dataSource.setJdbcUrl(url);

        try (var connection = dataSource.getConnection()) {
        }

        LOG.info("connect4 end");
    }

    private static String encode(String prefix, String key, String value) {
        return prefix + URLEncoder.encode(key, StandardCharsets.UTF_8) + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
