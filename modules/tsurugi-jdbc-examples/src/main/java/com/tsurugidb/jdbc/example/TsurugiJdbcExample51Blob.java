/*
 * Copyright 2025-2026 Project Tsurugi.
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.tsurugidb.jdbc.TsurugiJdbcLobTransferType;
import com.tsurugidb.jdbc.connection.TsurugiJdbcConnection;

/**
 * Tsurugi JDBC Blob example.
 */
public class TsurugiJdbcExample51Blob {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample51Blob.class);

    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException, IOException {
        var info = new Properties();
        info.setProperty("user", "tsurugi");
        info.setProperty("password", "password");
        info.setProperty("defaultTimeout", "10"); // seconds

        info.setProperty("lobTransferType", "DEFAULT");

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
            dropTable(connection);
            createTable(connection);

            var lobTransferType = connection.unwrap(TsurugiJdbcConnection.class).getLobTransferType();
            LOG.info("lobTransferType={}", lobTransferType);

            if (lobTransferType != TsurugiJdbcLobTransferType.NOT_USE) {
                insert(connection);
                selectBlob(connection);
                selectBytes(connection);
            }
        }
    }

    static void dropTable(Connection connection) throws SQLException {
        LOG.info("dropTable() start");
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.createStatement()) {
            String sql = "drop table if exists test";
            int r = statement.executeUpdate(sql);
            LOG.info("dropTable().count={}", r);
        }

        LOG.info("dropTable() end");
    }

    static void createTable(Connection connection) throws SQLException {
        LOG.info("createTable() start");
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.createStatement()) {
            String sql = "create table test (" //
                    + " pk int primary key," //
                    + " value blob" //
                    + ")";
            int r = statement.executeUpdate(sql);
            LOG.info("createTable().count={}", r);
        }

        LOG.info("createTable() end");
    }

    static void insert(Connection connection) throws SQLException, IOException {
        LOG.info("insert() start");
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.prepareStatement("insert into test (pk, value) values (?, ?)")) {
            int count = 0;
            for (int i = 0; i < 3; i++) {
                try (var is = new ByteArrayInputStream(new byte[] { (byte) i, 0x01, 0x02 })) {
                    statement.setInt(1, i);
                    statement.setBlob(2, is);
                    count += statement.executeUpdate();
                }
            }
            LOG.info("insert.count={}", count);
        }

        LOG.info("insert() end");
    }

    static void selectBlob(Connection connection) throws SQLException, IOException {
        LOG.info("selectBlob() start");
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.createStatement()) {
            var rs = statement.executeQuery("select * from test order by pk");
            while (rs.next()) {
                int pk = rs.getInt("pk");
                byte[] value;
                {
                    Blob blob = rs.getBlob("value");
                    if (blob == null) {
                        value = null;
                    } else {
                        try (var is = blob.getBinaryStream()) {
                            value = is.readAllBytes();
                        }
                    }
                }
                LOG.info("pk={}, value={}", pk, Arrays.toString(value));
            }
        }

        LOG.info("selectBlob() end");
    }

    static void selectBytes(Connection connection) throws SQLException, IOException {
        LOG.info("selectBytes() start");
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.createStatement()) {
            var rs = statement.executeQuery("select * from test order by pk");
            while (rs.next()) {
                int pk = rs.getInt("pk");
                byte[] value = rs.getBytes("value");
                LOG.info("pk={}, value={}", pk, Arrays.toString(value));
            }
        }

        LOG.info("selectBytes() end");
    }
}
