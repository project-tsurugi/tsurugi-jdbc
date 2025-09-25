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
package com.tsurugidb.jdbc.statement;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcNotSupported;
import com.tsurugidb.jdbc.exception.TsurugiJdbcExceptionHandler;
import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.util.TsurugiJdbcSqlTypeUtil;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;

/**
 * Tsurugi JDBC ParameterMetadata.
 */
public class TsurugiJdbcParameterMetaData implements ParameterMetaData {

    private final TsurugiJdbcPreparedStatement ownerPreparedStatement;

    /**
     * Creates a new instance.
     *
     * @param ownerPreparedStatement prepared statement
     */
    public TsurugiJdbcParameterMetaData(TsurugiJdbcPreparedStatement ownerPreparedStatement) {
        this.ownerPreparedStatement = Objects.requireNonNull(ownerPreparedStatement);
    }

    /**
     * Get factory.
     *
     * @return factory
     */
    protected TsurugiJdbcFactory getFactory() {
        return ownerPreparedStatement.getFactory();
    }

    /**
     * Get exception handler.
     *
     * @return exception handler
     */
    protected TsurugiJdbcExceptionHandler getExceptionHandler() {
        return getFactory().getExceptionHandler();
    }

    /**
     * Get SQL type utility.
     *
     * @return SQL type utility
     */
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

    /**
     * Get low-level placeholder list.
     *
     * @return low-level placeholder list
     */
    protected List<Placeholder> getLowPlaceholderList() {
        return ownerPreparedStatement.getLowPlaceholderList();
    }

    /**
     * Get low-level placeholder.
     *
     * @param param parameter number (1-origin)
     * @return low-level placeholder
     * @throws SQLException if the parameter number is out of range
     */
    protected Placeholder getLowPlaceholder(int param) throws SQLException {
        int index = param - 1;

        var lowPlaceholderList = getLowPlaceholderList();
        try {
            return lowPlaceholderList.get(index);
        } catch (Exception e) {
            throw getExceptionHandler().sqlException("getLowPlaceholder error", e);
        }
    }

    @Override
    public int getParameterCount() throws SQLException {
        var lowPlaceholderList = getLowPlaceholderList();
        return lowPlaceholderList.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        getLowPlaceholder(param);
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        var lowPlaceholder = getLowPlaceholder(param);
        var atomType = lowPlaceholder.getAtomType();
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
    @TsurugiJdbcNotSupported
    public int getPrecision(int param) throws SQLException {
        getLowPlaceholder(param);
        return 0;
    }

    @Override
    @TsurugiJdbcNotSupported
    public int getScale(int param) throws SQLException {
        getLowPlaceholder(param);
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        var lowPlaceholder = getLowPlaceholder(param);
        var atomType = lowPlaceholder.getAtomType();

        var util = getSqlTypeUtil();
        var jdbcType = util.toJdbcType(atomType);
        return jdbcType.getVendorTypeNumber();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        var lowPlaceholder = getLowPlaceholder(param);
        var atomType = lowPlaceholder.getAtomType();

        var util = getSqlTypeUtil();
        return util.toSqlTypeName(atomType);
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        var lowPlaceholder = getLowPlaceholder(param);
        var atomType = lowPlaceholder.getAtomType();

        var util = getSqlTypeUtil();
        var type = util.toJavaClass(atomType);
        return type.getCanonicalName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        getLowPlaceholder(param);
        return parameterModeIn;
    }
}
