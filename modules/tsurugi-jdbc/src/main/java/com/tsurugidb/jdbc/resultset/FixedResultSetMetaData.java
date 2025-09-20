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

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;

public class FixedResultSetMetaData implements ResultSetMetaData {

    private final FixedResultSet ownerResultSet;
    private final List<FixedResultSetColumn> columnList;

    @TsurugiJdbcInternal
    public FixedResultSetMetaData(FixedResultSet ownerResultSet, List<FixedResultSetColumn> columnList) {
        this.ownerResultSet = ownerResultSet;
        this.columnList = columnList;
    }

    public TsurugiJdbcFactory getFactory() {
        return ownerResultSet.getFactory();
    }

    protected TsurugiJdbcSqlTypeUtil getSqlTypeUtil() {
        return getFactory().getSqlTypeUtil();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw getFactory().getExceptionHandler().unwrapException(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnList.size();
    }

    @TsurugiJdbcInternal
    public FixedResultSetColumn getColumn(int column) throws SQLException {
        int index = column - 1;
        try {
            return columnList.get(index);
        } catch (IndexOutOfBoundsException e) {
            throw getFactory().getExceptionHandler().sqlException("getColumn error", e);
        }
    }

    @TsurugiJdbcInternal
    public JDBCType getJdbcType(int column) throws SQLException {
        var rawColumn = getColumn(column);
        return rawColumn.type();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        var rawColumn = getColumn(column);
        if (rawColumn.nullable()) {
            return columnNullable;
        } else {
            return columnNoNulls;
        }
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        var jdbcType = getJdbcType(column);
        switch (jdbcType) {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case REAL:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case NUMERIC:
            return true;
        default:
            return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        var jdbcType = getJdbcType(column);
        switch (jdbcType) {
        case BOOLEAN:
            return 1;
        case TINYINT:
            return 4;
        case SMALLINT:
            return 6;
        case INTEGER:
            return 11;
        case BIGINT:
            return 20;
        case REAL:
        case FLOAT:
            return 15;
        case DOUBLE:
            return 25;
        case DECIMAL:
        case NUMERIC:
            return getPrecision(column);
        case CHAR:
        case VARCHAR:
        case LONGNVARCHAR:
            return getLength(column);
        case BINARY:
        case VARBINARY:
        case LONGVARBINARY:
            return getLength(column) * 2;
        default:
            throw new UnsupportedOperationException(); // TODO SQLException
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        var rawColumn = getColumn(column);
        return rawColumn.name();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    public int getLength(int column) throws SQLException{
        var rawColumn = getColumn(column);
        return rawColumn.length();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        var rawColumn = getColumn(column);
        return rawColumn.precision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        var rawColumn = getColumn(column);
        return rawColumn.scale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        var jdbcType = getJdbcType(column);
        return jdbcType.getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        var jdbcType = getJdbcType(column);
        return jdbcType.getName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        var type = getColumnClass(column);
        return type.getName();
    }

    protected Class<?> getColumnClass(int column) throws SQLException {
        var jdbcType = getJdbcType(column);
        switch (jdbcType) {
        case BOOLEAN:
            return boolean.class;
        case TINYINT:
            return byte.class;
        case SMALLINT:
            return short.class;
        case INTEGER:
            return int.class;
        case BIGINT:
            return long.class;
        case REAL:
        case FLOAT:
            return float.class;
        case DOUBLE:
            return double.class;
        case DECIMAL:
        case NUMERIC:
            return BigDecimal.class;
        case CHAR:
        case VARCHAR:
        case LONGVARCHAR:
            return String.class;
        case BINARY:
        case VARBINARY:
        case LONGVARBINARY:
            return byte[].class;
        default:
            throw new UnsupportedOperationException(); // TODO SQLException
        }
    }
}
