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
package com.tsurugidb.jdbc.util.type;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Optional;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;

/**
 * Tsurugi type.
 */
public interface TsurugiJdbcType {

    /**
     * Get atom type.
     *
     * @return atom type
     * @throws SQLException if type is not supported
     */
    public AtomType getAtomType() throws SQLException;

    /**
     * Get varying.
     *
     * @return varying
     */
    public default boolean isVarying() {
        return findVarying().orElse(true);
    }

    /**
     * Get varying.
     *
     * @return varying
     */
    public Optional<Boolean> findVarying();

    /**
     * Get length.
     *
     * @return length
     */
    public Optional<ArbitraryInt> findLength();

    /**
     * Get precision.
     *
     * @return precision
     */
    public Optional<ArbitraryInt> findPrecision();

    /**
     * Get scale.
     *
     * @return scale
     */
    public Optional<ArbitraryInt> findScale();

    /**
     * Get nullable.
     *
     * @return nullable
     */
    public Optional<Boolean> findNullable();

    /**
     * Get JDBCType.
     *
     * @return JDBCType
     * @throws SQLException if type is not supported
     */
    public default JDBCType getJdbcType() throws SQLException {
        var atomType = getAtomType();
        switch (atomType) {
        case CHARACTER:
            if (isVarying()) {
                return JDBCType.VARCHAR;
            } else {
                return JDBCType.CHAR;
            }
        case OCTET:
            if (isVarying()) {
                return JDBCType.VARBINARY;
            } else {
                return JDBCType.BINARY;
            }
        default:
            return getJdbcType(atomType);
        }
    }

    /**
     * Convert from AtomType to JDBCType.
     *
     * @param atomType atom type
     * @return JDBCType
     * @throws SQLException if type is not supported
     */
    public static JDBCType getJdbcType(AtomType atomType) throws SQLException {
        switch (atomType) {
        case BOOLEAN:
            return JDBCType.BOOLEAN;
        case INT4:
            return JDBCType.INTEGER;
        case INT8:
            return JDBCType.BIGINT;
        case FLOAT4:
            return JDBCType.REAL;
        case FLOAT8:
            return JDBCType.DOUBLE;
        case DECIMAL:
            return JDBCType.DECIMAL;
        case CHARACTER:
            return JDBCType.VARCHAR;
        case OCTET:
            return JDBCType.VARBINARY;
        case DATE:
            return JDBCType.DATE;
        case TIME_OF_DAY:
            return JDBCType.TIME;
        case TIME_POINT:
            return JDBCType.TIMESTAMP;
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return JDBCType.TIME_WITH_TIMEZONE;
        case TIME_POINT_WITH_TIME_ZONE:
            return JDBCType.TIMESTAMP_WITH_TIMEZONE;
        case BLOB:
            return JDBCType.BLOB;
        case CLOB:
            return JDBCType.CLOB;
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported AtomType.{0}", atomType));
        }
    }

    /**
     * Get SQL type name.
     *
     * @return SQL type name
     * @throws SQLException if type is not supported
     */
    public default String getSqlTypeName() throws SQLException {
        var jdbcType = getJdbcType();
        return getSqlTypeName(jdbcType);
    }

    /**
     * Convert from JDBCType to SQL type name.
     *
     * @param jdbcType JDBC type
     * @return SQL type name
     * @throws SQLException if type is not supported
     */
    public static String getSqlTypeName(JDBCType jdbcType) throws SQLException {
        switch (jdbcType) {
        case INTEGER:
            return "INT";
        case TIME_WITH_TIMEZONE:
            return "TIME WITH TIME ZONE";
        case TIMESTAMP_WITH_TIMEZONE:
            return "TIMESTAMP WITH TIME ZONE";
        default:
            return jdbcType.name();
        }
    }

    /**
     * Convert from AtomType to Java class.
     *
     * @return Java class
     * @throws SQLException if atom type is not supported
     */
    public default Class<?> getJavaClass() throws SQLException {
        var atomType = getAtomType();
        switch (atomType) {
        case BOOLEAN:
            return boolean.class;
        case INT4:
            return int.class;
        case INT8:
            return long.class;
        case FLOAT4:
            return float.class;
        case FLOAT8:
            return double.class;
        case DECIMAL:
            return BigDecimal.class;
        case CHARACTER:
            return String.class;
        case OCTET:
            return byte[].class;
        case DATE:
            return LocalDate.class;
        case TIME_OF_DAY:
            return LocalTime.class;
        case TIME_POINT:
            return LocalDateTime.class;
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return OffsetTime.class;
        case TIME_POINT_WITH_TIME_ZONE:
            return OffsetDateTime.class;
        case BLOB:
            return java.sql.Blob.class;
        case CLOB:
            return java.sql.Clob.class;
        default:
            throw new SQLFeatureNotSupportedException(MessageFormat.format("Unsupported AtomType.{0}", atomType));
        }
    }
}
