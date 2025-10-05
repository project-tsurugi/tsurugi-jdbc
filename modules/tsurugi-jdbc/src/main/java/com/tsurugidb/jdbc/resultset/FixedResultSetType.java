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
import java.sql.SQLException;
import java.util.Optional;

import com.tsurugidb.jdbc.factory.TsurugiJdbcFactory;
import com.tsurugidb.jdbc.util.type.ArbitraryInt;
import com.tsurugidb.jdbc.util.type.TsurugiJdbcType;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

/**
 * Tsurugi type for {@link FixedResultSetColumn}.
 */
public class FixedResultSetType implements TsurugiJdbcType {

    private final JDBCType jdbcType;
    private final int length;
    private final int precision;
    private final int scale;
    private final boolean nullable;

    /**
     * Creates a new instance.
     *
     * @param jdbcType  JDBC type
     * @param length    length
     * @param precision precision
     * @param scale     scale
     * @param nullable  nullable
     */
    public FixedResultSetType(JDBCType jdbcType, int length, int precision, int scale, boolean nullable) {
        this.jdbcType = jdbcType;
        this.length = length;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    /**
     * Get JDBC type.
     *
     * @return JDBC type
     */
    public JDBCType jdbcType() {
        return this.jdbcType;
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

    @Override
    public AtomType getAtomType() throws SQLException {
        var typeUtil = TsurugiJdbcFactory.getDefaultFactory().getSqlTypeUtil();
        return typeUtil.toLowAtomType(jdbcType.getVendorTypeNumber());
    }

    @Override
    public Optional<Boolean> findVarying() {
        switch (this.jdbcType) {
        case VARCHAR:
        case NVARCHAR:
        case LONGVARCHAR:
        case LONGNVARCHAR:
        case VARBINARY:
        case LONGVARBINARY:
            return Optional.of(true);
        default:
            return Optional.of(false);
        }
    }

    @Override
    public Optional<ArbitraryInt> findLength() {
        return Optional.of(ArbitraryInt.of(this.length));
    }

    @Override
    public Optional<ArbitraryInt> findPrecision() {
        return Optional.of(ArbitraryInt.of(this.precision));
    }

    @Override
    public Optional<ArbitraryInt> findScale() {
        return Optional.of(ArbitraryInt.of(this.scale));
    }

    @Override
    public Optional<Boolean> findNullable() {
        return Optional.of(this.nullable);
    }
}
