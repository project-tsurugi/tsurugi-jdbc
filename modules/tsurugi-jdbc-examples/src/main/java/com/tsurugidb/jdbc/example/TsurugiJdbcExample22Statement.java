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
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Tsurugi JDBC Statement execute() example.
 */
public class TsurugiJdbcExample22Statement {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample22Statement.class);

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

        try (var statement = connection.createStatement()) {
            String sql = "drop table if exists test";
            boolean isQuery = statement.execute(sql);
            assert isQuery == false;

            int r = statement.getUpdateCount();
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
            boolean isQuery = statement.execute(sql);
            assert isQuery == false;

            int r = statement.getUpdateCount();
            LOG.info("createTable().count={}", r);
        }

        LOG.info("createTable() end");
    }

    static void insert(Connection connection) throws SQLException {
        LOG.info("insert() start");

        connection.setAutoCommit(false);
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.createStatement()) {
            for (int i = 0; i < 6; i++) {
                String sql;
                if (i < 3) {
                    sql = String.format("insert into test values(%d, %d, '%d')", i, i + 10, i);
                } else {
                    switch (i % 3) {
                    case 0:
                        sql = String.format("insert into test values(%d, null, null)", i);
                        break;
                    case 1:
                        sql = String.format("insert into test values(%d, null, '%d')", i, i);
                        break;
                    case 2:
                        sql = String.format("insert into test values(%d, %d, null)", i, i + 10);
                        break;
                    default:
                        throw new InternalError();
                    }
                }
                boolean isQuery = statement.execute(sql);
                assert isQuery == false;

                int r = statement.getUpdateCount();
                LOG.info("insert.count={}", r);
            }
        }

        connection.commit();

        LOG.info("insert() end");
    }

    static void select(Connection connection) throws SQLException {
        LOG.info("select() start");

        connection.setAutoCommit(true);
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.createStatement()) {
            String sql = "select * from test order by foo";
            boolean isQuery = statement.execute(sql);
            assert isQuery == true;

            try (var rs = statement.getResultSet()) {
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

        LOG.info("select() end");
    }

    static void selectName(Connection connection) throws SQLException {
        LOG.info("selectName() start");

        connection.setAutoCommit(true);
        LOG.info("autoCommit={}", connection.getAutoCommit());

        try (var statement = connection.createStatement()) {
            String sql = "select * from test order by foo";
            boolean isQuery = statement.execute(sql);
            assert isQuery == true;

            try (var rs = statement.getResultSet()) {
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

        LOG.info("selectName() end");
    }
}
