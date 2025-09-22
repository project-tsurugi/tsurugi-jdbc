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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

public class TsurugiJdbcResultSetMetaData implements ResultSetMetaData {

    private final TsurugiJdbcResultSet ownerResultSet;
    private final com.tsurugidb.tsubakuro.sql.ResultSetMetadata lowMetadata;

    @TsurugiJdbcInternal
    public TsurugiJdbcResultSetMetaData(TsurugiJdbcResultSet ownerResultSet, com.tsurugidb.tsubakuro.sql.ResultSetMetadata lowMetadata) {
        this.ownerResultSet = ownerResultSet;
        this.lowMetadata = lowMetadata;
    }

    public TsurugiJdbcFactory getFactory() {
        return ownerResultSet.getFactory();
    }

    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    protected TsurugiJdbcSqlTypeUtil getSqlTypeUtil() {
        return getFactory().getSqlTypeUtil();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw getExceptionHandler().unwrapException(iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @Override
    public int getColumnCount() throws SQLException {
        var lowColumnList = lowMetadata.getColumns();
        return lowColumnList.size();
    }

    @TsurugiJdbcInternal
    public SqlCommon.Column getLowColumn(int column) throws SQLException {
        int index = column - 1;
        var lowColumnList = lowMetadata.getColumns();
        try {
            return lowColumnList.get(index);
        } catch (IndexOutOfBoundsException e) {
            throw getExceptionHandler().sqlException("getLowColumn error", e);
        }
    }

    @TsurugiJdbcInternal
    public AtomType getLowAtomType(int column) throws SQLException {
        var lowColumn = getLowColumn(column);
        return lowColumn.getAtomType();
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
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        var nullable = util.findNullable(lowColumn);
        if (nullable.isPresent()) {
            if (nullable.get()) {
                return columnNullable;
            } else {
                return columnNoNulls;
            }
        }
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        var atomType = getLowAtomType(column);
        switch (atomType) {
        case INT4:
        case INT8:
        case FLOAT4:
        case FLOAT8:
        case DECIMAL:
            return true;
        default:
            return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        return util.toDisplaySize(lowColumn);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        var lowColumn = getLowColumn(column);
        return lowColumn.getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    public int getLength(int column) throws SQLException {
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        return util.getLength(lowColumn);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        return util.getPrecision(lowColumn);
    }

    @Override
    public int getScale(int column) throws SQLException {
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        return util.getScale(lowColumn);
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
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        var jdbcType = util.toJdbcType(lowColumn);
        return jdbcType.getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        return util.toSqlTypeName(lowColumn);
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
        var lowColumn = getLowColumn(column);
        var util = getSqlTypeUtil();
        var type = util.toJavaClass(lowColumn);
        return type.getName();
    }
}
