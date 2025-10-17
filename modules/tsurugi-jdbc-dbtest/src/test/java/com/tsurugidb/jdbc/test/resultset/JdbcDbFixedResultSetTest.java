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
package com.tsurugidb.jdbc.test.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.tsurugidb.jdbc.resultset.FixedResultSet;
import com.tsurugidb.jdbc.test.util.JdbcDbTester;

/**
 * {@link FixedResultSet} test.
 */
public class JdbcDbFixedResultSetTest extends JdbcDbTester {

    @Test
    void relative() throws SQLException {
        try (var connection = createConnection()) {
            var metaData = connection.getMetaData();
            var nameList = new ArrayList<String>();
            try (var rs = metaData.getTypeInfo()) {
                assertTrue(rs.isBeforeFirst());
                assertFalse(rs.isFirst());
                assertFalse(rs.isLast());
                assertFalse(rs.isAfterLast());

                while (rs.next()) {
                    String name = rs.getString("TYPE_NAME");
                    nameList.add(name);
                }

                assertFalse(rs.isBeforeFirst());
                assertFalse(rs.isFirst());
                assertFalse(rs.isLast());
                assertTrue(rs.isAfterLast());

                int rowNumber;
                {
                    rowNumber = nameList.size() / 2;
                    assertTrue(rs.absolute(rowNumber));
                    assertEquals(nameList.get(rowNumber - 1), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    assertTrue(rs.relative(-1));
                    rowNumber--;
                    assertEquals(nameList.get(rowNumber - 1), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    assertTrue(rs.relative(+3));
                    rowNumber += 3;
                    assertEquals(nameList.get(rowNumber - 1), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
            }
        }
    }

    @Test
    void absolute() throws SQLException {
        try (var connection = createConnection()) {
            var metaData = connection.getMetaData();
            var nameList = new ArrayList<String>();
            try (var rs = metaData.getTypeInfo()) {
                while (rs.next()) {
                    String name = rs.getString("TYPE_NAME");
                    nameList.add(name);
                }

                assertFalse(rs.isBeforeFirst());
                assertFalse(rs.isFirst());
                assertFalse(rs.isLast());
                assertTrue(rs.isAfterLast());

                int rowNumber;
                {
                    rowNumber = 0;
                    assertFalse(rs.absolute(rowNumber));
                    assertTrue(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    rowNumber = 1;
                    assertTrue(rs.absolute(rowNumber));
                    assertEquals(nameList.get(rowNumber - 1), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertTrue(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    rowNumber = 2;
                    assertTrue(rs.absolute(rowNumber));
                    assertEquals(nameList.get(rowNumber - 1), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    rowNumber = nameList.size();
                    assertTrue(rs.absolute(rowNumber));
                    assertEquals(nameList.get(rowNumber - 1), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertTrue(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    rowNumber = nameList.size() + 1;
                    assertFalse(rs.absolute(rowNumber));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertTrue(rs.isAfterLast());
                }

                {
                    rowNumber = -1;
                    assertTrue(rs.absolute(rowNumber));
                    assertEquals(nameList.get(nameList.size() + rowNumber), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertTrue(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    rowNumber = -2;
                    assertTrue(rs.absolute(rowNumber));
                    assertEquals(nameList.get(nameList.size() + rowNumber), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
            }
        }
    }

    @Test
    void first() throws SQLException {
        try (var connection = createConnection()) {
            var metaData = connection.getMetaData();
            var nameList = new ArrayList<String>();
            try (var rs = metaData.getTypeInfo()) {
                while (rs.next()) {
                    String name = rs.getString("TYPE_NAME");
                    nameList.add(name);
                }

                assertFalse(rs.isBeforeFirst());
                assertFalse(rs.isFirst());
                assertFalse(rs.isLast());
                assertTrue(rs.isAfterLast());

                int rowNumber;
                {
                    assertTrue(rs.first());
                    rowNumber = 1;
                    assertEquals(nameList.get(rowNumber - 1), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertTrue(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    assertFalse(rs.previous());
                    assertTrue(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
            }
        }
    }

    @Test
    void last() throws SQLException {
        try (var connection = createConnection()) {
            var metaData = connection.getMetaData();
            var nameList = new ArrayList<String>();
            try (var rs = metaData.getTypeInfo()) {
                while (rs.next()) {
                    String name = rs.getString("TYPE_NAME");
                    nameList.add(name);
                }

                assertFalse(rs.isBeforeFirst());
                assertFalse(rs.isFirst());
                assertFalse(rs.isLast());
                assertTrue(rs.isAfterLast());

                int rowNumber;
                {
                    assertTrue(rs.last());
                    rowNumber = nameList.size() - 1;
                    assertEquals(nameList.get(rowNumber), rs.getString("TYPE_NAME"));
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertTrue(rs.isLast());
                    assertFalse(rs.isAfterLast());
                }
                {
                    assertFalse(rs.next());
                    assertFalse(rs.isBeforeFirst());
                    assertFalse(rs.isFirst());
                    assertFalse(rs.isLast());
                    assertTrue(rs.isAfterLast());
                }
            }
        }
    }
}
