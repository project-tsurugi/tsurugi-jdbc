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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Tsurugi JDBC Statement executeUpdate(), executeQuery() example.
 */
public class TsurugiJdbcExample31DatabaseMetaData {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample31DatabaseMetaData.class);

    private static final String JDBC_URL = "jdbc:tsurugi:tcp://localhost:12345";

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        var info = new Properties();
        info.setProperty("user", "tsurugi");
        info.setProperty("password", "password");
        info.setProperty("defaultTimeout", "10"); // seconds

        try (var connection = DriverManager.getConnection(JDBC_URL, info)) {
            dropTable(connection);
            createTable(connection);

            var metadata = connection.getMetaData();
            getTables(metadata);
            getColumns(metadata);
            getPrimaryKeys(metadata);
            getTypeInfo(metadata);
            getClientInfoProperties(metadata);
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
                    + " foo int primary key," //
                    + " bar bigint," //
                    + " zzz varchar(10)" //
                    + ")";
            int r = statement.executeUpdate(sql);
            LOG.info("createTable().count={}", r);
        }

        LOG.info("createTable() end");
    }

    static void getTables(DatabaseMetaData metadata) throws SQLException {
        LOG.info("getTables() start");

        String[] types = { "TABLE" };
        try (var rs = metadata.getTables("", "", "", types)) {
            while (rs.next()) {
                String type = rs.getString("TABLE_TYPE");
                String tableName = rs.getString("TABLE_NAME");
                System.out.printf("%s\t%s%n", type, tableName);
            }
        }

        LOG.info("getTables() end");
    }

    static void getColumns(DatabaseMetaData metadata) throws SQLException {
        LOG.info("getColumns() start");

        try (var rs = metadata.getColumns("", "", "test", "%")) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                String type = rs.getString("TYPE_NAME");
                int position = rs.getInt("ORDINAL_POSITION");
                System.out.printf("%d. %s.%s\t%s%n", position, tableName, columnName, type);
            }
        }

        LOG.info("getColumns() end");
    }

    static void getPrimaryKeys(DatabaseMetaData metadata) throws SQLException {
        LOG.info("getPrimaryKeys() start");

        try (var rs = metadata.getPrimaryKeys("", "", "test")) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                int seq = rs.getInt("KEY_SEQ");
                System.out.printf("%d. %s.%s%n", seq, tableName, columnName);
            }
        }

        LOG.info("getPrimaryKeys() end");
    }

    static void getTypeInfo(DatabaseMetaData metadata) throws SQLException {
        LOG.info("getTypeInfo() start");

        try (var rs = metadata.getTypeInfo()) {
            while (rs.next()) {
                String typeName = rs.getString("TYPE_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                System.out.printf("%s\t%d%n", typeName, dataType);
            }
        }

        LOG.info("getTypeInfo() end");
    }

    static void getClientInfoProperties(DatabaseMetaData metadata) throws SQLException {
        LOG.info("getClientInfoProperties() start");

        try (var rs = metadata.getClientInfoProperties()) {
            while (rs.next()) {
                String name = rs.getString("NAME");
                System.out.printf("%s%n", name);
            }
        }

        LOG.info("getClientInfoProperties() end");
    }
}
