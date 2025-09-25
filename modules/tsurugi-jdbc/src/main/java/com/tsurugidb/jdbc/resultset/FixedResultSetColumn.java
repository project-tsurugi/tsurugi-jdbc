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
        return new FixedResultSetColumn(name, JDBCType.VARCHAR, 0, 0, 0, false);
    }

    /**
     * Create nullable string column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofStringNullable(String name) {
        return new FixedResultSetColumn(name, JDBCType.VARCHAR, 0, 0, 0, true);
    }

    /**
     * Create short column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofShort(String name) {
        return new FixedResultSetColumn(name, JDBCType.SMALLINT, 0, 0, 0, false);
    }

    /**
     * Create nullable short column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofShortNullable(String name) {
        return new FixedResultSetColumn(name, JDBCType.SMALLINT, 0, 0, 0, true);
    }

    /**
     * Create int column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofInt(String name) {
        return new FixedResultSetColumn(name, JDBCType.INTEGER, 0, 0, 0, false);
    }

    /**
     * Create nullable int column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofIntNullable(String name) {
        return new FixedResultSetColumn(name, JDBCType.INTEGER, 0, 0, 0, true);
    }

    /**
     * Create boolean column.
     *
     * @param name column name
     * @return column
     */
    public static FixedResultSetColumn ofBoolean(String name) {
        return new FixedResultSetColumn(name, JDBCType.BOOLEAN, 0, 0, 0, false);
    }

    private final String name;
    private final JDBCType type;
    private final int length;
    private final int precision;
    private final int scale;
    private final boolean nullable;

    /**
     * Creates a new instance.
     *
     * @param name      column name
     * @param type      JDBC type
     * @param length    length
     * @param precision precision
     * @param scale     scale
     * @param nullable  nullable
     */
    public FixedResultSetColumn(String name, JDBCType type, int length, int precision, int scale, boolean nullable) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
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
     * Get JDBC type.
     *
     * @return JDBC type
     */
    public JDBCType type() {
        return this.type;
    }

    /**
     * Get length.
     *
     * @return length
     */
    public int length() {
        return this.length;
    }

    /**
     * Get precision.
     *
     * @return precision
     */
    public int precision() {
        return this.precision;
    }

    /**
     * Get scale.
     *
     * @return scale
     */
    public int scale() {
        return this.scale;
    }

    /**
     * Get nullable.
     *
     * @return true if nullable, false otherwise
     */
    public boolean nullable() {
        return this.nullable;
    }
}
