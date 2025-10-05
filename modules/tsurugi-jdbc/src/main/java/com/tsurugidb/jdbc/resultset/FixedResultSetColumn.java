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
package com.tsurugidb.jdbc.resultset;

import java.sql.JDBCType;

/**
 * {@link FixedResultSet} Column.
 */
public class FixedResultSetColumn {

    /**
     * Create string column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofString(String name) {
        var type = new FixedResultSetType(JDBCType.VARCHAR, 0, 0, 0, false);
        return new FixedResultSetColumn(name, type);
    }

    /**
     * Create nullable string column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofStringNullable(String name) {
        var type = new FixedResultSetType(JDBCType.VARCHAR, 0, 0, 0, true);
        return new FixedResultSetColumn(name, type);
    }

    /**
     * Create short column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofShort(String name) {
        var type = new FixedResultSetType(JDBCType.SMALLINT, 0, 0, 0, false);
        return new FixedResultSetColumn(name, type);
    }

    /**
     * Create nullable short column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofShortNullable(String name) {
        var type = new FixedResultSetType(JDBCType.SMALLINT, 0, 0, 0, true);
        return new FixedResultSetColumn(name, type);
    }

    /**
     * Create int column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofInt(String name) {
        var type = new FixedResultSetType(JDBCType.INTEGER, 0, 0, 0, false);
        return new FixedResultSetColumn(name, type);
    }

    /**
     * Create nullable int column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofIntNullable(String name) {
        var type = new FixedResultSetType(JDBCType.INTEGER, 0, 0, 0, true);
        return new FixedResultSetColumn(name, type);
    }

    /**
     * Create boolean column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofBoolean(String name) {
        var type = new FixedResultSetType(JDBCType.BOOLEAN, 0, 0, 0, false);
        return new FixedResultSetColumn(name, type);
    }

    private final String name;
    private final FixedResultSetType type;

    /**
     * Creates a new instance.
     *
     * @param name      column name
     * @param type      tsurugi type
     */
    public FixedResultSetColumn(String name, FixedResultSetType type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Get column name.
     *
     * @return column name
     */
    public String name() {
        return this.name;
    }

    /**
     * Get tsurugi type.
     *
     * @return tsurugi type
     */
    public FixedResultSetType type() {
        return this.type;
    }
}
