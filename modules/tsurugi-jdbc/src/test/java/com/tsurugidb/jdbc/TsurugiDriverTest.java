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
package com.tsurugidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

class TsurugiDriverTest {

    private static Driver getDriver() throws SQLException {
        return DriverManager.getDriver("jdbc:tsurugi:hoge");
    }

    @Test
    void acceptsURL() throws SQLException {
        var driver = DriverManager.getDriver("jdbc:tsurugi:hoge");
        assertNotNull(driver);
        assertInstanceOf(TsurugiDriver.class, driver);

        assertThrows(SQLException.class, () -> {
            DriverManager.getDriver("jdbc:tsurugidb:hoge");
        });
    }

    @Test
    void getMajorVersion() throws SQLException {
        var driver = getDriver();

        int version = driver.getMajorVersion();
        assertTrue(version >= 0, "getMajorVersion() error");
    }

    @Test
    void getMinorVersion() throws SQLException {
        var driver = getDriver();

        int version = driver.getMinorVersion();
        assertTrue(version >= 0, "getMinorVersion() error");
    }
}
