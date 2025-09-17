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

import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.tsurugidb.jdbc.TsurugiDriver;

public class TsurugiJdbcExample99Driver {

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws SQLFeatureNotSupportedException {
        var driver = getTsurugiDriver();
        var logger = driver.getParentLogger();
        logger.info("log example");
    }

    static TsurugiDriver getTsurugiDriver() {
        var enumeration = DriverManager.getDrivers();
        while (enumeration.hasMoreElements()) {
            var driver = enumeration.nextElement();

            if (driver instanceof TsurugiDriver) {
                return (TsurugiDriver) driver;
            }
        }
        throw new RuntimeException("TsurugiDriver not found");
    }
}
