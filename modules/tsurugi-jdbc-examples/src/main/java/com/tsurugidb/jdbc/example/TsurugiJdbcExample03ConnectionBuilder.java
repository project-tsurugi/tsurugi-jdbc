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

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.tsurugidb.jdbc.TsurugiDataSource;

public class TsurugiJdbcExample03ConnectionBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiJdbcExample03ConnectionBuilder.class);

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLException {
        connect();
    }

    static void connect() throws SQLException {
        LOG.info("connect start");

        var dataSource = new TsurugiDataSource();
        var builder = dataSource.createConnectionBuilder() //
                .endpoint("tcp://localhost:12345") //
                .user("tsurugi") //
                .password("password") //
        ;

        try (var connection = builder.build()) {
        }

        LOG.info("connect end");
    }
}
