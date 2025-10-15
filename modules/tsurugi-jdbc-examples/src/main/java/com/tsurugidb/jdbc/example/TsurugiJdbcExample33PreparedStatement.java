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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Tsurugi JDBC PreparedStatement executeBatch() example.
 */
public class TsurugiJdbcExample33PreparedStatement {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample33PreparedStatement.class);

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
            insert(connection);
            select(connection);
            selectName(connection);
        }
    }

    static void dropTable(Connection connection) throws SQLException {
        LOG.info("dropTable() start");
        LOG.info("autoCommit={}", connection.getAutoCommit());

        String sql = "drop table if exists test";
        try (var ps = connection.prepareStatement(sql)) {
            boolean isQuery = ps.execute();
            assert isQuery == false;

            int r = ps.getUpdateCount();
            LOG.info("dropTable().count={}", r);
        }

        LOG.info("dropTable() end");
    }

    static void createTable(Connection connection) throws SQLException {
        LOG.info("createTable() start");
        LOG.info("autoCommit={}", connection.getAutoCommit());

        String sql = "create table test (" //
                + " foo int primary key," //
                + " bar bigint," //
                + " zzz varchar(10)" //
                + ")";
        try (var ps = connection.prepareStatement(sql)) {
            boolean isQuery = ps.execute();
            assert isQuery == false;

            int r = ps.getUpdateCount();
            LOG.info("createTable().count={}", r);
        }

        LOG.info("createTable() end");
    }

    static void insert(Connection connection) throws SQLException {
        LOG.info("insert() start");

        connection.setAutoCommit(true);
        LOG.info("autoCommit={}", connection.getAutoCommit());

        String sql = "insert into test values(?, ?, ?)";
        try (var ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < 6; i++) {
                ps.setInt(1, i);
                if (i < 3) {
                    ps.setLong(2, i + 10);
                    ps.setString(3, Integer.toString(i));
                } else {
                    switch (i % 3) {
                    case 0:
                        ps.setNull(2, Types.BIGINT);
                        ps.setNull(3, Types.VARCHAR);
                        break;
                    case 1:
                        ps.setNull(2, Types.BIGINT);
                        ps.setString(3, Integer.toString(i));
                        break;
                    case 2:
                        ps.setLong(2, i + 10);
                        ps.setNull(3, Types.VARCHAR);
                        break;
                    default:
                        throw new InternalError();
                    }
                }

                ps.addBatch();
            }

            int[] r = ps.executeBatch();
            LOG.info("insert.count={}", Arrays.toString(r));
        }

        LOG.info("insert() end");
    }

    static void select(Connection connection) throws SQLException {
        LOG.info("select() start");

        connection.setAutoCommit(true);
        LOG.info("autoCommit={}", connection.getAutoCommit());

        String sql = "select * from test where foo=?";
        try (var ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < 6; i++) {
                ps.setInt(1, i);

                boolean isQuery = ps.execute();
                assert isQuery == true;

                try (var rs = ps.getResultSet()) {
                    while (rs.next()) {
                        var result = new ArrayList<Object>();

                        int foo = rs.getInt(1);
                        assert rs.wasNull() == false;
                        result.add(foo);

                        long bar = rs.getLong(2);
                        if (rs.wasNull()) {
                            assert bar == 0L;
                            result.add(null);
                        } else {
                            result.add(bar);
                        }

                        String zzz = rs.getString(3);
                        assert rs.wasNull() == (zzz == null);
                        result.add(zzz);

                        System.out.println(result);
                    }
                }
            }
        }

        LOG.info("select() end");
    }

    static void selectName(Connection connection) throws SQLException {
        LOG.info("selectName() start");

        connection.setAutoCommit(true);
        LOG.info("autoCommit={}", connection.getAutoCommit());

        String sql = "select * from test where foo=?";
        try (var ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < 6; i++) {
                ps.setInt(1, i);

                boolean isQuery = ps.execute();
                assert isQuery == true;

                try (var rs = ps.getResultSet()) {
                    while (rs.next()) {
                        var result = new ArrayList<Object>();

                        int foo = rs.getInt("foo");
                        assert rs.wasNull() == false;
                        result.add(foo);

                        long bar = rs.getLong("bar");
                        if (rs.wasNull()) {
                            assert bar == 0L;
                            result.add(null);
                        } else {
                            result.add(bar);
                        }

                        String zzz = rs.getString("zzz");
                        assert rs.wasNull() == (zzz == null);
                        result.add(zzz);

                        System.out.println(result);
                    }
                }
            }
        }

        LOG.info("selectName() end");
    }
}
